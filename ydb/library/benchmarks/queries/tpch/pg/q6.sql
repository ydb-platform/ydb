{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG


select
    sum(l_extendedprice * l_discount) as revenue
from
    {{lineitem}}
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.0100001 and 0.06 + 0.0100001
    and l_quantity < 24;
