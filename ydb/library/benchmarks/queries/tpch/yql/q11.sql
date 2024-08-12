{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$j1 = (
select
    s.s_suppkey as s_suppkey
from
    {{supplier}} as s
join
    {{nation}} as n
on
    n.n_nationkey = s.s_nationkey
where
    n.n_name = 'CANADA'
);

$j2 = (
select
    ps.ps_partkey as ps_partkey,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_availqty as ps_availqty
from
    {{partsupp}} as ps
join
    $j1 as j
on
    ps.ps_suppkey = j.s_suppkey
);

$threshold = (
select
    sum(ps_supplycost * ps_availqty) * $z0_0001_35 as threshold
from
    $j2
);

$values = (
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    $j2
group by
    ps_partkey
);

select
    v.ps_partkey as ps_partkey,
    v.value as value
from
    $values as v
cross join
    $threshold as t
where
    v.value > t.threshold
order by
    value desc;
