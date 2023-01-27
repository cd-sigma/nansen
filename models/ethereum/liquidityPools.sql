select
    concat("UNISWAP V2 POOL: ", token0.symbol, " + ", token1.symbol) as poolname,
    concat('0x', substring(log.data, 27, 40)) as pooladdress,
    concat('0x', substring(log.topics[offset(1)], 27, 40)) as token0,
    concat('0x', substring(log.topics[offset(2)], 27, 40)) as token1,
    token0.symbol as symbol0,
    token1.symbol as symbol1

from `bigquery-public-data.crypto_ethereum.logs` as log
join
    `bigquery-public-data.crypto_ethereum.tokens` as token0
    on token0.address = concat('0x', substring(log.topics[offset(1)], 27, 40))
join
    `bigquery-public-data.crypto_ethereum.tokens` as token1
    on token1.address = concat('0x', substring(log.topics[offset(2)], 27, 40))
where
    log.address = "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
    and log.topics[offset(0)]
    = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
union all
select
    concat("SUSHISWAP POOL: ", token0.symbol, " + ", token1.symbol) as poolname,
    concat('0x', substring(log.data, 27, 40)) as pooladdress,
    concat('0x', substring(log.topics[offset(1)], 27, 40)) as token0,
    concat('0x', substring(log.topics[offset(2)], 27, 40)) as token1,
    token0.symbol as symbol0,
    token1.symbol as symbol1

from `bigquery-public-data.crypto_ethereum.logs` as log
join
    `bigquery-public-data.crypto_ethereum.tokens` as token0
    on token0.address = concat('0x', substring(log.topics[offset(1)], 27, 40))
join
    `bigquery-public-data.crypto_ethereum.tokens` as token1
    on token1.address = concat('0x', substring(log.topics[offset(2)], 27, 40))
where
    log.address = "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"
    and log.topics[offset(0)]
    = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
