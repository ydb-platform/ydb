
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    ps.ps_partkey as ps_partkey,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_availqty as ps_availqty,
    s.s_nationkey as s_nationkey
from
    `/Root/partsupp` as ps
join
    `/Root/supplier` as s
on
    ps.ps_suppkey = s.s_suppkey
);
$join2 = (
select
    j.ps_partkey as ps_partkey,
    j.ps_supplycost as ps_supplycost,
    j.ps_availqty as ps_availqty,
    j.s_nationkey as s_nationkey
from
    $join1 as j
join
    `/Root/nation` as n
on
    n.n_nationkey = j.s_nationkey
where
    n.n_name = 'CANADA'
);
$threshold = (
select
    sum(ps_supplycost * ps_availqty) * 0.0001000000 as threshold
from
    $join2
);
$values = (
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    $join2
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
