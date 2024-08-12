{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    {{lineitem}}
    cross join {{part}}
where
    l_partkey = p_partkey
    and l_shipdate >= date('1994-08-01')
    and l_shipdate < date('1994-08-01') + interval('P31D');
