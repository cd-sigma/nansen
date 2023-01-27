SELECT
  address,
  SUM(value) AS balance
FROM (
  SELECT
    token_address,
    from_address AS address,
    -CAST(value AS bigdecimal) / 1e18 AS value
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers`
  UNION ALL
  SELECT
    token_address,
    to_address AS address,
    CAST(value AS bigdecimal) / 1e18 AS value
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers` )
WHERE
  token_address = '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72'
GROUP BY
  address
ORDER BY
  balance DESC;