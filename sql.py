trader_classifier_query = """
WITH address_activity AS (
  SELECT
    ORIGIN_FROM_ADDRESS,
    COUNT(*) AS tx_count,
    SUM(AMOUNT_IN_USD) AS total_volume_usd,
    AVG(AMOUNT_IN_USD) AS avg_order_size_usd,
    MIN(BLOCK_TIMESTAMP) AS first_activity,
    MAX(BLOCK_TIMESTAMP) AS last_activity,
    COUNT(DISTINCT CONTRACT_ADDRESS) AS unique_contracts
  FROM
    arbitrum.defi.ez_dex_swaps
  WHERE
    platform = 'uniswap-v3'
  GROUP BY
    ORIGIN_FROM_ADDRESS
),
CLASSIFIER AS (
  SELECT
    ORIGIN_FROM_ADDRESS,
    tx_count,
    total_volume_usd,
    avg_order_size_usd,
    first_activity,
    last_activity,
    unique_contracts,
    CASE
      WHEN total_volume_usd > 1000000
      OR avg_order_size_usd > 10000
      OR unique_contracts > 20 THEN 'Professional'
      ELSE 'Retail'
    END AS trader_type
  FROM
    address_activity
  WHERE
    total_volume_usd
    or avg_order_size_usd is not null
)
SELECT
  C.TRADER_TYPE,
  date_trunc('HOUR', S.block_timestamp) as DT,
  COUNT(*) AS tx_count,
  SUM(AMOUNT_IN_USD) AS total_volume_usd,
  AVG(AMOUNT_IN_USD) AS avg_order_size_usd,
  COUNT(DISTINCT CONTRACT_ADDRESS) AS unique_contracts
FROM
  CLASSIFIER C
  JOIN arbitrum.defi.ez_dex_swaps S ON C.ORIGIN_FROM_ADDRESS = S.origin_from_address
WHERE
  S.platform = 'uniswap-v3'
  and date_trunc('day', s.block_timestamp) >= '2023-07-01'
GROUP BY
  C.TRADER_TYPE,
  DATE_TRUNC('HOUR', S.block_timestamp)
ORDER BY
  DT DESC,
  TRADER_TYPE



"""

prices_and_vol_query = """
with eth_vol as (
  select
    date_trunc('hour', block_timestamp) as dt,
    sum(AMOUNT_IN_USD) as total_volume
  from
    arbitrum.defi.ez_dex_swaps
  where
    date_trunc('day', block_timestamp) >= '2023-07-01'
    and platform != 'uniswap-v3'
  group by
    date_trunc('hour', block_timestamp)
  order by
    dt desc
),
eth_btc_prices as (
  select
    hour as dt,
    symbol,
    price
  from
    ethereum.price.ez_prices_hourly
  where
    date_trunc('day', hour) >= '2023-07-01'
    and symbol in('WETH', 'WBTC')
  order by
    dt desc
)
select
  e.dt,
  eb.symbol,
  eb.price,
  e.total_volume as arbitrum_vol_ex_uni
from
  eth_vol e
  left join eth_btc_prices eb on e.dt = eb.dt
order by
  e.dt desc




"""