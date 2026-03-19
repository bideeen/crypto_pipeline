-- models/intermediate/int_daily_returns.sql
--
-- Purpose: Calculate derived metrics per coin.
-- Adds market sentiment classification and volume anomaly detection.
--
-- Why intermediate and not directly in marts?
-- These calculations might be reused by multiple mart models.
-- We calculate once here, reference many times downstream.

with prices as (
    select * from {{ ref('stg_coin_prices') }}
),

with_metrics as (
    select
        coin_id,
        coin_symbol,
        coin_name,
        price_usd,
        high_24h_usd,
        low_24h_usd,
        market_cap_usd,
        market_cap_rank,
        volume_24h_usd,
        price_change_pct_24h,
        circulating_supply,
        last_updated_at,
        ingested_at,

        -- How wide was today's price range as % of price?
        -- High range = volatile day
        round(
            ((high_24h_usd - low_24h_usd) / price_usd) * 100,
            2
        ) as price_range_pct,

        -- Volume to market cap ratio
        -- High ratio = a lot of the coin is being traded today
        round(
            (volume_24h_usd / nullif(market_cap_usd, 0)) * 100,
            2
        ) as volume_to_mcap_pct,

        -- Market sentiment based on 24h price change
        case
            when price_change_pct_24h >=  10 then 'strong_bullish'
            when price_change_pct_24h >=   3 then 'bullish'
            when price_change_pct_24h >=  -3 then 'neutral'
            when price_change_pct_24h >= -10 then 'bearish'
            else 'strong_bearish'
        end as market_sentiment,

        -- Is this coin showing unusual volume today?
        case
            when (volume_24h_usd / nullif(market_cap_usd, 0)) > 0.3
                then 'very_high'
            when (volume_24h_usd / nullif(market_cap_usd, 0)) > 0.1
                then 'high'
            when (volume_24h_usd / nullif(market_cap_usd, 0)) > 0.05
                then 'normal'
            else 'low'
        end as volume_activity

    from prices
)

select * from with_metrics