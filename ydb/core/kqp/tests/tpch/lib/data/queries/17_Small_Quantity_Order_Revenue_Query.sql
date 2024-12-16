-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$threshold = (
    select
        0.2 * avg(l_quantity) as threshold,
        l_partkey
    from
        `$DBROOT$/lineitem`
    group by
        l_partkey
);

$join1 = (
    select
        p.p_partkey as p_partkey,
        l.l_extendedprice as l_extendedprice,
        l.l_quantity as l_quantity
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/part` as p on p.p_partkey = l.l_partkey
    where
        p.p_brand = 'Brand#24'
        and p.p_container = 'MED CAN'
);

select
    sum(j.l_extendedprice) / 7.0 as avg_yearly
from
    $threshold as t
join
    $join1 as j
on
    j.p_partkey = t.l_partkey
where
    j.l_quantity < t.threshold;
