select to_address as contract_address, block_number as block_number
from `bigquery-public-data.crypto_ethereum.traces`
where trace_type = "create" and to_address is not null
