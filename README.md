# Crypto Market Intelligence Pipeline

A production-style data engineering project that ingests, transforms,
tests, and serves live crypto market data daily.

Built to practice real engineering workflows — not just local scripts.

---

## What this does

Fetches the top 20 coins by market cap from CoinGecko every morning,
loads them into DuckDB, runs dbt transformations across three layers,
validates data quality with 7 automated tests, and logs everything.

---

## Architecture
```
CoinGecko API
     ↓
fetch_coins.py        # Pulls raw JSON, partitions by date
     ↓
data/raw/date=YYYY-MM-DD/coins.json
     ↓
load_raw.py           # Loads JSON into DuckDB raw schema
     ↓
stg_coin_prices       # Renames columns, casts types
     ↓
int_daily_returns     # Calculates sentiment, volume ratios
     ↓
mart_market_summary   # Final table for dashboards
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion | Python, requests |
| Storage | DuckDB |
| Transformation | dbt-core, dbt-duckdb |
| Data quality | dbt tests |
| Orchestration | Windows Task Scheduler → Airflow (planned) |
| Version control | Git |

---

## Running it
```bash
# Set up environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run full pipeline manually
python run_pipeline.py

# Run dbt only
cd dbt_project
dbt run
dbt test
```

Scheduled automatically at 07:00 daily via Task Scheduler.
Logs written to logs/pipeline.log.

---

## Data layers

| Model | Type | Description |
|---|---|---|
| stg_coin_prices | View | Cleaned raw API data |
| int_daily_returns | View | Sentiment and volume metrics |
| mart_market_summary | Table | Final reporting table |

---

## Engineering decisions

**Why DuckDB?** Zero infrastructure. Runs in a file. Identical SQL
to production warehouses like BigQuery. Swap the adapter when ready
to go to cloud.

**Why separate ingestion and loading?** Fetching from an API and
writing to a database are different failure modes. Keeping them
separate means you can rerun the load without hitting the API again.

**Why halt on failure?** Silent partial runs are worse than no run.
If step 2 fails, step 3 should never run on stale data.

**Why UTC everywhere?** Timezones at the storage layer cause
date-boundary bugs that are nearly impossible to debug in production.

---

## Project structure
```
crypto_pipeline/
├── ingestion/
│   ├── fetch_coins.py      # CoinGecko API ingestion
│   └── load_raw.py         # DuckDB loader
├── dbt_project/
│   └── models/
│       ├── staging/        # stg_coin_prices
│       ├── intermediate/   # int_daily_returns
│       └── marts/          # mart_market_summary
├── logs/                   # Pipeline run logs (gitignored)
├── data/                   # Raw JSON + DuckDB file (gitignored)
├── run_pipeline.py         # Master pipeline runner
├── run_pipeline.bat        # Windows scheduler entry point
└── CHANGELOG.md            # All schema and pipeline changes
```