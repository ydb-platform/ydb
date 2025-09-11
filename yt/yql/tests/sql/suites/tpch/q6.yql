
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1995-01-01");

select
    sum(l_extendedprice * l_discount) as revenue
from
    plato.lineitem
where
    CAST(l_shipdate AS Timestamp) >= $border 
    and cast(l_shipdate as Timestamp) < ($border + Interval("P365D"))
    and l_discount between 0.07 - 0.01 and 0.07 + 0.01
    and l_quantity < 25;
