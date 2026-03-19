-- models/staging/stg_coin_prices.sql
--
-- Purpose: Clean and rename raw CoinGecko data.
-- This is the ONLY place raw column names are referenced.
-- Every model downstream uses these clean names.
--
-- If CoinGecko ever renames a field, we fix it here.
-- Nothing else needs to change.

with source as (
    select * from {{ source('raw', 'coin_prices') }}
),

renamed as (
    select
        -- Identifiers
        id                                          as coin_id,
        symbol                                      as coin_symbol,
        name                                        as coin_name,

        -- Pricing
        cast(current_price as double)               as price_usd,
        cast(high_24h as double)                    as high_24h_usd,
        cast(low_24h as double)                     as low_24h_usd,

        -- Market data
        cast(market_cap as double)                  as market_cap_usd,
        cast(market_cap_rank as integer)            as market_cap_rank,
        cast(total_volume as double)                as volume_24h_usd,

        -- Price changes
        cast(price_change_24h as double)            as price_change_24h_usd,
        cast(price_change_percentage_24h as double) as price_change_pct_24h,

        -- Supply
        cast(circulating_supply as double)          as circulating_supply,
        cast(total_supply as double)                as total_supply,

        -- Timestamps
        cast(last_updated as timestamp)             as last_updated_at,
        cast(ingested_at as timestamp with time zone)
                                                    as ingested_at

    from source
)

select * from renamed