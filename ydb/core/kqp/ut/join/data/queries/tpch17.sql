-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$threshold = (
select
    0.2 * avg(l_quantity) as threshold,
    l.l_partkey as l_partkey
from
    `/Root/lineitem` as l
join
    `/Root/part` as p
on
    p.p_partkey = l.l_partkey
where
    p.p_brand = 'Brand#35'
    and p.p_container = 'LG DRUM'
group by
    l.l_partkey
);

select
    sum(l.l_extendedprice) / 7.0 as avg_yearly
from
    `/Root/lineitem` as l
join
    $threshold as t
on
    t.l_partkey = l.l_partkey
where
    l.l_quantity < t.threshold;