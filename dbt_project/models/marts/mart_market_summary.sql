-- models/marts/mart_market_summary.sql
--
-- Purpose: Final market summary table for dashboards and reporting.
-- One row per coin per ingestion run.
--
-- This is the table stakeholders query.
-- Column names must be self-explanatory to a non-engineer.
-- No raw field names. No ambiguous abbreviations.

with daily_returns as (
    select * from {{ ref('int_daily_returns') }}
),

final as (
    select
        -- Identity
        coin_id,
        coin_symbol,
        coin_name,

        -- Pricing
        price_usd,
        high_24h_usd,
        low_24h_usd,
        price_change_pct_24h,
        price_range_pct,

        -- Market standing
        market_cap_usd,
        market_cap_rank,

        -- Volume
        volume_24h_usd,
        volume_to_mcap_pct,
        volume_activity,

        -- Signals
        market_sentiment,

        -- Metadata
        last_updated_at,
        ingested_at,

        -- Pipeline audit field
        -- Lets you know exactly when dbt wrote this row
        -- current_timestamp as dbt_updated_at
        timezone('UTC', current_timestamp) as dbt_updated_at

    from daily_returns
)

select * from final