{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-08-01");
select
    $z100_35 * sum(case
        when p.p_type like 'PROMO%'
            then l.l_extendedprice * ($z1_12 - l.l_discount)
        else $z0_12
    end) / sum(l.l_extendedprice * ($z1_12 - l.l_discount)) as promo_revenue
from
    {{lineitem}} as l
join
    {{part}} as p
on
    l.l_partkey = p.p_partkey
where
    l.l_shipdate >= $border
    and l.l_shipdate < ($border + Interval("P31D"));