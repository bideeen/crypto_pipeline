# Changelog

All pipeline changes are documented here.
Format: [version] — YYYY-MM-DD

---

## [1.0.0] — 2026-03-19

### Added
- CoinGecko ingestion script with date-partitioned JSON storage
- DuckDB loader with temp file handling and idempotent writes
- dbt staging model: stg_coin_prices — column renaming and type casting
- dbt intermediate model: int_daily_returns — sentiment and volume classification
- dbt mart model: mart_market_summary — dashboard-ready with UTC audit timestamp
- 7 data quality tests across staging layer
- Master pipeline runner with ordered execution and hard failure on error
- Windows Task Scheduler automation at 07:00 daily
- Structured logging to logs/pipeline.log

### Design decisions
- DuckDB chosen over cloud warehouse for zero-infrastructure local development
- Raw data stored as JSON before loading — preserves original API response
- UTC enforced at storage layer, not display layer
- Pipeline halts immediately on any step failure — no silent partial runs

---

## [Unreleased]

### Planned
- Airflow DAG to replace batch script
- Historical data accumulation (append instead of overwrite)
- dbt intermediate tests for sentiment and volume columns
- Cloud storage migration to GCS