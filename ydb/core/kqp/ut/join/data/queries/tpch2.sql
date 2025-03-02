-- PRAGMA ydb.OptShuffleElimination = 'true';
-- PRAGMA ydb.OptShuffleEliminationWithMap = 'true';

$n = (
select
    n_name,
    n_nationkey
from
    nation as n
);

$min_ps_supplycost = (
select
    min(ps_supplycost) as min_ps_supplycost,
    ps.ps_availqty as ps_partkey1
from
    partsupp as ps
group by
    ps.ps_availqty
);

$p = (
select
    p_partkey,
    p_mfgr
from
    part
where
    p_size = 15
    and p_type like '%BRASS'
);

$ps = (
select
    ps.ps_partkey as ps_partkey2,
    p.p_mfgr as p_mfgr,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_suppkey as ps_suppkey
from
    partsupp as ps
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
    supplier as s
join
    $n as n
on
    s.s_nationkey = n.n_nationkey
);

$jp = (
select
    ps_partkey2,  -- использую алиас из предыдущего субзапроса
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
    jp.ps_partkey2 as p_partkey,  -- использую алиас из предыдущего субзапроса
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    $jp as jp
join
    $min_ps_supplycost as m
on
    jp.ps_partkey2 = m.ps_partkey1
where
    min_ps_supplycost = ps_supplycost
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;