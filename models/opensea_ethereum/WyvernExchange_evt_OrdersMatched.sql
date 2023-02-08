CREATE TEMP FUNCTION
  PARSE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`buyHash` STRING, `sellHash` STRING, `maker` STRING, `taker` STRING, `price` STRING, `metadata` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": false, "name": "buyHash", "type": "bytes32"}, {"indexed": false, "name": "sellHash", "type": "bytes32"}, {"indexed": true, "name": "maker", "type": "address"}, {"indexed": true, "name": "taker", "type": "address"}, {"indexed": false, "name": "price", "type": "uint256"}, {"indexed": true, "name": "metadata", "type": "bytes32"}], "name": "OrdersMatched", "type": "event"}
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
WHERE (address = '0x7f268357a8c2552623316e2562d90e642bb538e5' or address = '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b')
  AND topics[SAFE_OFFSET(0)] = '0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9'
)
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,log_index
    ,parsed.buyHash AS `buyHash`
    ,parsed.sellHash AS `sellHash`
    ,parsed.maker AS `maker`
    ,parsed.taker AS `taker`
    ,parsed.price AS `price`
    ,parsed.metadata AS `metadata`
FROM parsed_logs