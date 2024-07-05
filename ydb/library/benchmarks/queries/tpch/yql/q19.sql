{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    sum(l.l_extendedprice* ($z1_12 - l.l_discount)) as revenue
from
    {{lineitem}} as l
join
    {{part}} as p
on
    p.p_partkey = l.l_partkey
where
    (
        p.p_brand = 'Brand#23'
        and (p.p_container = 'SM CASE' or p.p_container = 'SM BOX' or p.p_container = 'SM PACK' or p.p_container = 'SM PKG')
        and l.l_quantity >= 7 and l.l_quantity <= 7 + 10
        and p.p_size between 1 and 5
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#15'
        and (p.p_container = 'MED BAG' or p.p_container = 'MED BOX' or p.p_container = 'MED PKG' or p.p_container = 'MED PACK')
        and l.l_quantity >= 17 and l.l_quantity <= 17 + 10
        and p.p_size between 1 and 10
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#44'
        and (p.p_container = 'LG CASE' or p.p_container = 'LG BOX' or p.p_container = 'LG PACK' or p.p_container = 'LG PKG')
        and l.l_quantity >= 25 and l.l_quantity <= 25 + 10
        and p.p_size between 1 and 15
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    );
