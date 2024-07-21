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