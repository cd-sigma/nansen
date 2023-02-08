CREATE TEMP FUNCTION
    PARSE_TRACE(data STRING)
    RETURNS STRUCT<`addrs` STRING, `uints` STRING, `feeMethodsSidesKindsHowToCalls` STRING, `calldataBuy` STRING, `calldataSell` STRING, `replacementPatternBuy` STRING, `replacementPatternSell` STRING, `staticExtradataBuy` STRING, `staticExtradataSell` STRING, `vs` STRING, `rssMetadata` STRING, error STRING>
    LANGUAGE js AS """
    var abi = {"constant": false, "inputs": [{"name": "addrs", "type": "address[14]"}, {"name": "uints", "type": "uint256[18]"}, {"name": "feeMethodsSidesKindsHowToCalls", "type": "uint8[8]"}, {"name": "calldataBuy", "type": "bytes"}, {"name": "calldataSell", "type": "bytes"}, {"name": "replacementPatternBuy", "type": "bytes"}, {"name": "replacementPatternSell", "type": "bytes"}, {"name": "staticExtradataBuy", "type": "bytes"}, {"name": "staticExtradataSell", "type": "bytes"}, {"name": "vs", "type": "uint8[2]"}, {"name": "rssMetadata", "type": "bytes32[5]"}], "name": "atomicMatch_", "outputs": [], "payable": true, "stateMutability": "payable", "type": "function"};
    var interface_instance = new ethers.utils.Interface([abi]);

    var result = {};
    try {
        var parsedTransaction = interface_instance.parseTransaction({data: data});
        var parsedArgs = parsedTransaction.args;

        if (parsedArgs && parsedArgs.length >= abi.inputs.length) {
            for (var i = 0; i < abi.inputs.length; i++) {
                var paramName = abi.inputs[i].name;
                var paramValue = parsedArgs[i];
                if (abi.inputs[i].type === 'address' && typeof paramValue === 'string') {
                    // For consistency all addresses are lowercase.
                    paramValue = paramValue.toLowerCase();
                }
                result[paramName] = paramValue;
            }
        } else {
            result['error'] = 'Parsed transaction args is empty or has too few values.';
        }
    } catch (e) {
        result['error'] = e.message;
    }

    return result;
"""
OPTIONS
  ( library="gs://blockchain-etl-bigquery/ethers.js" );

WITH parsed_traces AS
(SELECT
    traces.block_timestamp AS block_timestamp
    ,traces.block_number AS block_number
    ,traces.transaction_hash AS transaction_hash
    ,traces.trace_address AS trace_address
    ,PARSE_TRACE(traces.input) AS parsed
FROM `bigquery-public-data.crypto_ethereum.traces` AS traces
WHERE (to_address = '0x7f268357a8c2552623316e2562d90e642bb538e5' OR to_address = '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b')
  AND STARTS_WITH(traces.input, '0xab834bab')
  )
SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,trace_address
     ,parsed.error AS error
     
    ,parsed.addrs AS `addrs`
    
    ,parsed.uints AS `uints`
    
    ,parsed.feeMethodsSidesKindsHowToCalls AS `feeMethodsSidesKindsHowToCalls`
    
    ,parsed.calldataBuy AS `calldataBuy`
    
    ,parsed.calldataSell AS `calldataSell`
    
    ,parsed.replacementPatternBuy AS `replacementPatternBuy`
    
    ,parsed.replacementPatternSell AS `replacementPatternSell`
    
    ,parsed.staticExtradataBuy AS `staticExtradataBuy`
    
    ,parsed.staticExtradataSell AS `staticExtradataSell`
    
    ,parsed.vs AS `vs`
    
    ,parsed.rssMetadata AS `rssMetadata`
    
FROM parsed_traces