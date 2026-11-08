# CANESTREET API

A small, read-only web service that gives canestreet.com live data from the
DB_FREELANCE_MARKET database. It can only read, never change, the database.

## What it serves

| Address | Used for |
|---|---|
| `/stats` | Open contracts today, new this week, last update |
| `/contracts?industry=&role_type=&page=1` | The open-contract list, 20 per page, newest first |
| `/filters` | Industries and role types that have open contracts |
| `/market?weeks=12&industry=` | Weekly new contracts, open by industry and role type, top roles, remote/hybrid share |
| `/health` | Render's uptime check |

It reads `canonical_contracts` (open = `current_status` is `open`), the
newest row per contract in `enrichments` (`domains` as industry,
`work_model`, `seniority`, `duration_months`, `location_city`),
`canonical_listing_links` + `raw_listings` (the link to the original
listing) and `source_runs` (last update time). Role type is worked out from
the contract title with Danish and English keywords (`ROLE_RULES` in
`main.py`); titles that match nothing are "Other". Answers are cached for
5 minutes.

## Deploy on Render

1. Put these four files in a GitHub repository (for example `canestreet-api`).
2. In Render: **New → Blueprint**, pick the repository. Render reads `render.yaml`.
   (Or **New → Web Service**: build `pip install -r requirements.txt`,
   start `uvicorn main:app --host 0.0.0.0 --port $PORT`, region Frankfurt.)
3. When asked for `DATABASE_URL`, paste the database's **Internal Database URL**
   (DB_FREELANCE_MARKET → Connect → Internal). Never put it in the code.
4. Deploy. Open `https://<your-service>.onrender.com/stats` to check it.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `ALLOWED_ORIGINS` | canestreet.com with and without www | Websites allowed to call the API |
| `ALLOWED_ORIGIN_REGEX` | any `*.squarespace.com` | Lets the new site work on its temporary address |
| `OPEN_STATUSES` | `open` | `current_status` values that count as open (comma-separated) |
| `ENRICH_INDUSTRY_KEY` | `domains` | Key in `enrichment_json` used as industry (text or list; first entry used) |
| `ENRICH_ROLE_KEY` | empty | Optional key in `enrichment_json` with a role type; empty = derive from title |
| `CACHE_SECONDS` | `300` | How long answers are cached |

Change a setting in Render → the service → Environment, then redeploy.
