-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1996-01-01");

select
    sum(l_extendedprice * l_discount) as revenue
from
    `$DBROOT$/lineitem`
where
    CAST(l_shipdate AS Timestamp) >= $border
    and cast(l_shipdate as Timestamp) < ($border + Interval("P365D"))
    and l_discount between 0.06 and 0.08
    and l_quantity < 25;
