"""CANESTREET read-only API.

Serves open IT freelance contracts and market statistics from the
DB_FREELANCE_MARKET Postgres database to the canestreet.com website.

Every database session is forced read-only; the API can never change data.
"""

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def _list(name: str, default: str) -> list[str]:
    return [v.strip() for v in os.environ.get(name, default).split(",") if v.strip()]


DATABASE_URL = os.environ["DATABASE_URL"]
ALLOWED_ORIGINS = _list(
    "ALLOWED_ORIGINS", "https://www.canestreet.com,https://canestreet.com"
)
# Values of canonical_contracts.current_status that count as "open".
OPEN_STATUSES = _list("OPEN_STATUSES", "open")
# Key inside enrichments.enrichment_json used as the industry filter.
# It may hold a string or a list; for a list the first entry is used.
INDUSTRY_KEY = os.environ.get("ENRICH_INDUSTRY_KEY", "domains")
# Role type is derived from the contract title (there is no role field).
ROLE_KEY = os.environ.get("ENRICH_ROLE_KEY", "")

# Title keywords -> role type, checked in order (Danish and English).
ROLE_RULES = [
    ("Security", ["security", "sikkerhed", " iam ", "identity", "cyber", " soc ", "penetration", "pentest",
                  "informationssikkerhed", "grc", "compliance officer"]),
    ("Data & Analytics", ["data ", "data-", "dataengineer", "data engineer", "bi ", "bi-", "power bi",
                          "analytics", "analyst data", "machine learning", " ml ", "ai engineer", "data scientist"]),
    ("Architecture", ["architect", "arkitekt"]),
    ("Project & Delivery", ["project manager", "projektleder", "projekt leder", "program manager", "programleder",
                            "scrum", "agile coach", "pmo", "delivery", "release manager", "change manager",
                            "solution manager", "service manager", "it manager", "it-chef", "test manager"]),
    ("Testing", ["test", "qa ", "quality assurance"]),
    ("Infrastructure & Cloud", ["cloud", "devops", "azure", "aws", "infrastructure", "infrastruktur", "platform",
                                "network", "netværk", "drift", "sre", "kubernetes", "linux", "windows server"]),
    ("Business Analysis", ["business analyst", "forretningsanalytiker", "product owner", "produktejer",
                           "requirements", "process", "proces", "konsulent sap", "functional"]),
    ("Development", ["developer", "udvikler", "engineer", "programmør", "programmer", "frontend", "backend",
                     "fullstack", "full stack", ".net", "java", "python", "react", "sap", "salesforce"]),
]


def _role_case() -> str:
    whens = []
    for label, words in ROLE_RULES:
        conds = " OR ".join("lower(' ' || cc.title || ' ') LIKE '%%" + w.replace("'", "''") + "%%'" for w in words)
        whens.append(f"WHEN {conds} THEN '{label}'")
    return "CASE " + " ".join(whens) + " ELSE 'Other' END"


ROLE_EXPR = (
    f"COALESCE(NULLIF(le.ej ->> %(rk)s, ''), {_role_case()})" if ROLE_KEY else _role_case()
)
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", "300"))

pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=4,
    open=False,
    kwargs={
        "options": "-c default_transaction_read_only=on -c statement_timeout=10000",
        "row_factory": dict_row,
    },
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pool.open()
    yield
    pool.close()


app = FastAPI(title="CANESTREET API", lifespan=lifespan, docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=os.environ.get("ALLOWED_ORIGIN_REGEX") or None,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# One shared definition of a contract: the latest enrichment per contract
# supplies industry and role type.
BASE = (
    """
WITH latest_enrichment AS (
    SELECT DISTINCT ON (canonical_contract_id)
           canonical_contract_id, enrichment_json::jsonb AS ej
    FROM enrichments
    ORDER BY canonical_contract_id, id DESC
), c AS (
    SELECT cc.id, cc.contract_uid, cc.title, cc.customer,
           COALESCE(NULLIF(le.ej ->> 'location_city', ''), cc.location_text) AS location_text,
           cc.first_seen_at, cc.create_date, cc.close_date, cc.current_status,
           NULLIF(CASE jsonb_typeof(le.ej -> %(ik)s)
                      WHEN 'array' THEN le.ej -> %(ik)s ->> 0
                      ELSE le.ej ->> %(ik)s END, '') AS industry,
           """
    + ROLE_EXPR
    + """ AS role_type,
           NULLIF(le.ej ->> 'work_model', '') AS work_model,
           NULLIF(le.ej ->> 'seniority', '') AS seniority,
           CASE WHEN le.ej ->> 'duration_months' ~ '^[0-9]+(\\.[0-9]+)?$'
                THEN (le.ej ->> 'duration_months')::numeric END AS duration_months,
           (cc.current_status = ANY(%(open)s)) AS is_open,
           LEFT(cc.first_seen_at, 10)::date AS first_seen_day
    FROM canonical_contracts cc
    LEFT JOIN latest_enrichment le ON le.canonical_contract_id = cc.id
)
"""
)

_cache: dict[str, tuple[float, object]] = {}


def cached(key: str, build):
    hit = _cache.get(key)
    now = time.time()
    if hit and now - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = build()
    _cache[key] = (now, value)
    return value


def _fix_text(value):
    """Repair Danish letters stored with a broken encoding (e.g. 'KÃ¸benhavn')."""
    if isinstance(value, str) and ("Ã" in value or "Â" in value):
        try:
            return value.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value
    return value


def run(sql: str, params: dict | None = None) -> list[dict]:
    p = {"ik": INDUSTRY_KEY, "rk": ROLE_KEY, "open": OPEN_STATUSES}
    p.update(params or {})
    with pool.connection() as conn:
        rows = conn.execute(sql, p).fetchall()
    return [{k: _fix_text(v) for k, v in row.items()} for row in rows]


def filters_sql(industry: str | None, role_type: str | None) -> str:
    parts = ["is_open"]
    if industry:
        parts.append("industry = %(industry)s")
    if role_type:
        parts.append("role_type = %(role_type)s")
    return " AND ".join(parts)


@app.get("/health")
def health():
    run("SELECT 1 AS ok")
    return {"ok": True}


@app.get("/stats")
def stats():
    """Headline numbers: open contracts, new this week, last update."""

    def build():
        row = run(
            BASE
            + """
            SELECT COUNT(*) FILTER (WHERE is_open) AS open_contracts,
                   COUNT(*) FILTER (WHERE first_seen_day >= CURRENT_DATE - 7) AS new_this_week,
                   COUNT(*) FILTER (WHERE first_seen_day >= CURRENT_DATE - 14
                                      AND first_seen_day <  CURRENT_DATE - 7) AS new_last_week
            FROM c
            """
        )[0]
        updated = run(
            "SELECT MAX(completed_at) AS last_updated FROM source_runs WHERE success = 1"
        )[0]
        return {**row, **updated}

    return cached("stats", build)


@app.get("/filters")
def filters():
    """Industries and role types that currently have open contracts."""

    def build():
        def counts(col: str):
            return run(
                BASE
                + f"""
                SELECT {col} AS value, COUNT(*) AS count
                FROM c WHERE is_open AND {col} IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC, 1
                """
            )

        return {"industries": counts("industry"), "role_types": counts("role_type")}

    return cached("filters", build)


@app.get("/contracts")
def contracts(
    industry: str | None = None,
    role_type: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
):
    """Open contracts, newest first, filterable and paginated."""
    key = f"contracts:{industry}:{role_type}:{page}:{page_size}"

    def build():
        where = filters_sql(industry, role_type)
        params = {
            "industry": industry,
            "role_type": role_type,
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }
        total = run(BASE + f"SELECT COUNT(*) AS n FROM c WHERE {where}", params)[0]["n"]
        rows = run(
            BASE
            + f"""
            SELECT c.contract_uid AS id, c.title, c.customer, c.location_text AS location,
                   c.industry, c.role_type, c.work_model, c.seniority, c.duration_months,
                   c.create_date AS posted, c.close_date AS closes,
                   (SELECT rl.url
                      FROM canonical_listing_links l
                      JOIN raw_listings rl ON rl.id = l.raw_listing_id
                     WHERE l.canonical_contract_id = c.id AND rl.url IS NOT NULL
                     ORDER BY l.confidence DESC NULLS LAST, rl.id DESC
                     LIMIT 1) AS url
            FROM c
            WHERE {where}
            ORDER BY c.first_seen_at DESC NULLS LAST, c.id DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            params,
        )
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, -(-total // page_size)),
            "contracts": rows,
        }

    return cached(key, build)


@app.get("/market")
def market(
    weeks: int = Query(12, ge=4, le=104),
    industry: str | None = None,
):
    """Market insights: weekly new contracts, open by industry and role, top roles."""
    key = f"market:{weeks}:{industry}"

    def build():
        params = {"weeks": weeks, "industry": industry}
        ind = "AND industry = %(industry)s" if industry else ""
        weekly = run(
            BASE
            + f"""
            , w AS (
                SELECT generate_series(
                    date_trunc('week', CURRENT_DATE) - (%(weeks)s - 1) * INTERVAL '1 week',
                    date_trunc('week', CURRENT_DATE),
                    INTERVAL '1 week')::date AS week
            )
            SELECT w.week, COUNT(c.id) AS new_contracts
            FROM w
            LEFT JOIN c ON date_trunc('week', c.first_seen_day)::date = w.week {ind}
            GROUP BY w.week ORDER BY w.week
            """,
            params,
        )
        by_industry = run(
            BASE
            + """
            SELECT COALESCE(industry, 'Other') AS label, COUNT(*) AS count
            FROM c WHERE is_open GROUP BY 1 ORDER BY 2 DESC
            """
        )
        by_role = run(
            BASE
            + f"""
            SELECT COALESCE(role_type, 'Other') AS label, COUNT(*) AS count
            FROM c WHERE is_open {ind} GROUP BY 1 ORDER BY 2 DESC
            """,
            params,
        )
        top_roles = run(
            BASE
            + f"""
            SELECT title, role_type, COUNT(*) AS open_now
            FROM c WHERE is_open {ind}
            GROUP BY title, role_type ORDER BY 3 DESC, 1 LIMIT 5
            """,
            params,
        )
        remote = run(
            BASE
            + f"""
            SELECT COUNT(*) AS open_total,
                   COUNT(*) FILTER (WHERE COALESCE(work_model, location_text) ILIKE '%%remote%%'
                                       OR COALESCE(work_model, location_text) ILIKE '%%hybrid%%') AS remote_or_hybrid,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_months) AS median_duration_months
            FROM c WHERE is_open {ind}
            """,
            params,
        )[0]
        return {
            "weekly_new": weekly,
            "open_by_industry": by_industry,
            "open_by_role_type": by_role,
            "top_roles": top_roles,
            **remote,
        }

    return cached(key, build)


@app.exception_handler(Exception)
async def on_error(_request, exc):
    print("API error:", repr(exc))  # visible in Render's Logs tab
    return JSONResponse({"detail": "Data temporarily unavailable"}, status_code=503)
