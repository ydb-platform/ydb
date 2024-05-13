{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1995-01-01");

select
    sum(l_extendedprice * l_discount) as revenue
from
    {{lineitem}}
where
    l_shipdate >= $border 
    and l_shipdate < ($border + Interval("P365D"))
    and l_discount between 0.07 - 0.0100001 and 0.07 + 0.0100001
    and l_quantity < 25;
