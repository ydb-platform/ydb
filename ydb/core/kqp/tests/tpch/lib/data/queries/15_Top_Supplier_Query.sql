-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1996-07-01");

$revenue0 = (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue,
        cast(sum(l_extendedprice * (1 - l_discount)) as Uint64) as total_revenue_approx
    from
        `$DBROOT$/lineitem`
    where
        cast(l_shipdate as timestamp) >= $border
        and cast(l_shipdate as timestamp) < ($border + Interval("P93D"))
    group by
        l_suppkey
);

$max_revenue = (
    select
        max(total_revenue_approx) as max_revenue
    from
        $revenue0
);

$join1 = (
    select
        s.s_suppkey as s_suppkey,
        s.s_name as s_name,
        s.s_address as s_address,
        s.s_phone as s_phone,
        r.total_revenue as total_revenue,
        r.total_revenue_approx as total_revenue_approx
    from
        `$DBROOT$/supplier` as s join $revenue0 as r on s.s_suppkey = r.supplier_no
);

select
    j.s_suppkey as s_suppkey,
    j.s_name as s_name,
    j.s_address as s_address,
    j.s_phone as s_phone,
    j.total_revenue as total_revenue
from
    $join1 as j
join
    $max_revenue as m
on
    j.total_revenue_approx = m.max_revenue
order by
    s_suppkey;

