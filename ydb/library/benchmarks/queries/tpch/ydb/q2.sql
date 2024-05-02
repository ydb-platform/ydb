-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- using 1680793381 as a seed to the RNG

$r = (
select
    r_regionkey
from 
    `{path}region`
where
    r_name='AMERICA'
);

$n = (
select
    n_name,
    n_nationkey
from
    `{path}nation` as n 
left semi join
    $r as r
on 
    n.n_regionkey = r.r_regionkey
);

$s1 = (
select
    s_suppkey
from
    `{path}supplier` as s
left semi join
    $n as n
on
    s.s_nationkey = n.n_nationkey
);

$min_ps_supplycost = (
select
    min(ps_supplycost) as min_ps_supplycost,
    ps.ps_partkey as ps_partkey
from
    `{path}partsupp` as ps
left semi join
    $s1 as s
on
    ps.ps_suppkey = s.s_suppkey
group by
    ps.ps_partkey
);

$p = (
select
    p_partkey,
    p_mfgr
from
    `{path}part`
where
    p_size = 10
    and p_type like '%COPPER'
);

$ps = (
select
    ps.ps_partkey as ps_partkey,
    p.p_mfgr as p_mfgr,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_suppkey as ps_suppkey
from
    `{path}partsupp` as ps
join
    $p as p
on
    p.p_partkey = ps.ps_partkey
);

$s2 = (
select
    s_acctbal,
    s_name,
    s_address,
    s_phone,
    s_comment,
    s_suppkey,
    n_name
from
    `{path}supplier` as s
join
    $n as n
on
    s.s_nationkey = n.n_nationkey
);

$jp =(
select
    ps_partkey,
    ps_supplycost,
    p_mfgr,
    s_acctbal,
    s_name,
    s_address,
    s_phone,
    s_comment,
    n_name
from
    $ps as ps
join    
    $s2 as s
on
    ps.ps_suppkey = s.s_suppkey  
);

select
    s_acctbal,
    s_name,
    n_name,
    jp.ps_partkey as p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    $jp as jp
join
    $min_ps_supplycost as m
on
    jp.ps_partkey = m.ps_partkey
where
    min_ps_supplycost = ps_supplycost
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
