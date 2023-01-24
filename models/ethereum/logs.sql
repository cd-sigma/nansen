SELECT * FROM `bigquery-public-data.crypto_ethereum.logs` 
WHERE DATE(block_timestamp) = "2023-01-23" 
LIMIT 1000