
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$threshold = (
select
    0.2 * avg(l_quantity) as threshold,
    l_partkey
from
    plato.lineitem
group by
    l_partkey
);

$join1 = (
select
    p.p_partkey as p_partkey,
    l.l_extendedprice as l_extendedprice,
    l.l_quantity as l_quantity
from
    plato.lineitem as l
join
    plato.part as p
on
    p.p_partkey = l.l_partkey
where
    p.p_brand = 'Brand#35'
    and p.p_container = 'LG DRUM'
);

select
    sum(j.l_extendedprice) / 7.0 as avg_yearly
from
    $join1 as j
join
    $threshold as t
on
    j.p_partkey = t.l_partkey
where
    j.l_quantity < t.threshold;
