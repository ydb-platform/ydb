{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$p = select p_partkey from {{part}}
where
    p_brand = 'Brand#35'
    and p_container = 'LG DRUM'
;

$threshold = (
select
    $z0_2_12 * avg(l_quantity) as threshold,
    l.l_partkey as l_partkey
from
    {{lineitem}} as l
left semi join
    $p as p
on  
    p.p_partkey = l.l_partkey
group by
    l.l_partkey
);

$l = select l.l_partkey as l_partkey, l.l_quantity as l_quantity, l.l_extendedprice as l_extendedprice
from
    {{lineitem}} as l
join
    $p as p
on  
    p.p_partkey = l.l_partkey;

select
    sum(l.l_extendedprice) / $z7_35 as avg_yearly
from
    $l as l
join
    $threshold as t
on  
    t.l_partkey = l.l_partkey
where
    l.l_quantity < t.threshold;

