{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    sum(l_extendedprice* (1::numeric - l_discount)) as revenue
from
    {{lineitem}},
    {{part}}
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 7::numeric and l_quantity <= 7::numeric + 10::numeric
        and p_size::int4 between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#15'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 17::numeric and l_quantity <= 17::numeric + 10::numeric
        and p_size::int4 between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#44'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 25::numeric and l_quantity <= 25::numeric + 10::numeric
        and p_size::int4 between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );

