
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-08-01");
select
    100.00 * sum(case
        when StartsWith(p.p_type, 'PROMO')
            then l.l_extendedprice * (1 - l.l_discount)
        else 0
    end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
    plato.lineitem as l
join
    plato.part as p
on
    l.l_partkey = p.p_partkey
where
    cast(l.l_shipdate as timestamp) >= $border
    and cast(l.l_shipdate as timestamp) < ($border + Interval("P31D"));
