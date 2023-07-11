-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$exists = (
    select
        COUNT(*) > 0 as result,
        l1.l_orderkey as l_orderkey,
        l1.l_suppkey as l_suppkey
    from
        `{path}lineitem` as l1
    join
        `{path}lineitem` as l2
    on
        l2.l_orderkey = l1.l_orderkey
    where
        l2.l_suppkey <> l1.l_suppkey
    group by
        l1.l_orderkey, l1.l_suppkey
);
$not_exists_inverse = (
    select
        l1.l_orderkey as l_orderkey,
        l1.l_suppkey as l_suppkey
    from
        `{path}lineitem` as l1
    join
        `{path}lineitem` as l3
    on
        l3.l_orderkey = l1.l_orderkey
    where
        l3.l_suppkey <> l1.l_suppkey
        and l3.l_receiptdate > l3.l_commitdate
    group by
        l1.l_orderkey, l1.l_suppkey
);
$not_exists = (
    select
        l1.l_orderkey as l_orderkey,
        l1.l_suppkey as l_suppkey
    from
        `{path}lineitem` as l1
    left only join
        $not_exists_inverse AS nei
    on
        nei.l_orderkey = l1.l_orderkey and
        nei.l_suppkey = l1.l_suppkey
    group by
        l1.l_orderkey, l1.l_suppkey
);

select
    s.s_name as s_name,
    count(*) as numwait
from
    `{path}supplier` as s
join
    `{path}lineitem` as l1
on
    s.s_suppkey = l1.l_suppkey
join
    `{path}orders` as o
on
    o.o_orderkey = l1.l_orderkey
join
    `{path}nation` as n
on
    s.s_nationkey = n.n_nationkey
join
    $exists as e
    on e.l_orderkey == l1.l_orderkey and
      e.l_suppkey == l1.l_suppkey
join
    $not_exists as ne
    on ne.l_orderkey == l1.l_orderkey and
      ne.l_suppkey == l1.l_suppkey
where
    o.o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and n.n_name = 'EGYPT'
group by
    s.s_name
order by
    numwait desc,
    s_name
limit 100;
