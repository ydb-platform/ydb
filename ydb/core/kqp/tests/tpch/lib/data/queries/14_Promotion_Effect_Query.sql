-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1994-04-01");

select
    100.00 * sum(case
        when StartsWith(p.p_type, 'PROMO')
            then l.l_extendedprice * (1.0 - l.l_discount)
        else 0
    end) / sum(l.l_extendedprice * (1.0 - l.l_discount)) as promo_revenue
from
    `$DBROOT$/lineitem` as l join `$DBROOT$/part` as p on l.l_partkey = p.p_partkey
where
    cast(l.l_shipdate as timestamp) >= $border
    and cast(l.l_shipdate as timestamp) < ($border + Interval("P30D"));
