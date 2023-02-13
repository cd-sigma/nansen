CREATE TEMP FUNCTION
  PARSE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`punkIndex` STRING, `value` STRING, `fromAddress` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "name": "punkIndex", "type": "uint256"}, {"indexed": false, "name": "value", "type": "uint256"}, {"indexed": true, "name": "fromAddress", "type": "address"}], "name": "PunkBidEntered", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

WITH parsed_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,PARSE_LOG(logs.data, logs.topics) AS parsed
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
  AND topics[SAFE_OFFSET(0)] = '0x5b859394fabae0c1ba88baffe67e751ab5248d2e879028b8c8d6897b0519f56a'
)
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
    ,parsed.punkIndex AS `punkIndex`
    ,parsed.value AS `value`
    ,parsed.fromAddress AS `fromAddress`
FROM parsed_logs