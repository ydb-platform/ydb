{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-01-01");
$threshold = (
select
    $z0_5_35 * sum(l_quantity) as threshold,
    l_partkey as l_partkey,
    l_suppkey as l_suppkey
from
    {{lineitem}}
where
    l_shipdate >= $border
    and l_shipdate < ($border + Interval("P365D"))
group by
    l_partkey, l_suppkey
);

$parts = (
select
    p_partkey
from
    {{part}}
where
    p_name like 'maroon%'
);

$join1 = (
select
    ps.ps_suppkey as ps_suppkey,
    ps.ps_availqty as ps_availqty,
    ps.ps_partkey as ps_partkey
from
    {{partsupp}} as ps
join any
    $parts as p
on
    ps.ps_partkey = p.p_partkey
);

$join2 = (
select
    distinct(j.ps_suppkey) as ps_suppkey
from
    $join1 as j
join any
    $threshold as t
on
    j.ps_partkey = t.l_partkey and j.ps_suppkey = t.l_suppkey
where
    j.ps_availqty > t.threshold
);

$join3 = (
select
    j.ps_suppkey as ps_suppkey,
    s.s_name as s_name,
    s.s_address as s_address,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join any
    {{supplier}} as s
on
    j.ps_suppkey = s.s_suppkey
);

select
    j.s_name as s_name,
    j.s_address as s_address
from
    $join3 as j
join
    {{nation}} as n
on
    j.s_nationkey = n.n_nationkey
where
    n.n_name = 'VIETNAM'
order by
    s_name;

