CREATE TEMP FUNCTION
  PARSE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`tradeId` STRING, `buyer` STRING, `seller` STRING, `price` STRING, `proceeds` STRING, `cost` STRING, `currency` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "internalType": "bytes32", "name": "tradeId", "type": "bytes32"}, {"indexed": true, "internalType": "address", "name": "buyer", "type": "address"}, {"indexed": true, "internalType": "address", "name": "seller", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "price", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "proceeds", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "cost", "type": "uint256"}, {"indexed": false, "internalType": "contract IERC20", "name": "currency", "type": "address"}], "name": "Trade", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

WITH parsed_logs AS
(SELECT
     logs.block_timestamp AS evt_block_time
    ,logs.block_number AS evt_block_number
    ,logs.transaction_hash AS evt_tx_hash
    ,logs.log_index AS evt_index
    ,PARSE_LOG(logs.data, logs.topics) AS parsed
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address = '0x555598409fe9a72f0a5e423245c34555f6445555'
  AND topics[SAFE_OFFSET(0)] = '0x446ea7816fd82a3e830f82ef187f55d7718a739e8ecfcd33e456f69cfa164cb5'
)
SELECT
    "0x555598409fe9a72f0a5e423245c34555f6445555" as contract_address
     ,evt_block_time
     ,evt_block_number
     ,evt_tx_hash
     ,evt_index
    ,parsed.tradeId AS `tradeId`
    ,parsed.buyer AS `buyer`
    ,parsed.seller AS `seller`
    ,parsed.price AS `price`
    ,parsed.proceeds AS `proceeds`
    ,parsed.cost AS `cost`
    ,parsed.currency AS `currency`
FROM parsed_logs