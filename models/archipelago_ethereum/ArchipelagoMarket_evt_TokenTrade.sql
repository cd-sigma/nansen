CREATE TEMP FUNCTION
  PARSE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`tradeId` STRING, `tokenAddress` STRING, `tokenId` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "internalType": "bytes32", "name": "tradeId", "type": "bytes32"}, {"indexed": true, "internalType": "contract IERC721", "name": "tokenAddress", "type": "address"}, {"indexed": true, "internalType": "uint256", "name": "tokenId", "type": "uint256"}], "name": "TokenTrade", "type": "event"}
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
  AND topics[SAFE_OFFSET(0)] = '0x1db01bf65e5b6e3e603931112bbd1714e830813bdcf0b6ebf7b5dcf44542ca31'
)
SELECT
     "0x555598409fe9a72f0a5e423245c34555f6445555" as contract_address
     ,evt_block_time
     ,evt_block_number
     ,evt_tx_hash
     ,evt_index
    ,parsed.tradeId AS `tradeId`
    ,parsed.tokenAddress AS `tokenAddress`
    ,parsed.tokenId AS `tokenId`
FROM parsed_logs