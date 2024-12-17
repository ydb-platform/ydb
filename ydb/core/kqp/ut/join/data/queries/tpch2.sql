-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- using 1680793381 as a seed to the RNG

$r = (select r_regionkey from
    `/Root/region`
where r_name='AMERICA');

$j1 = (select n_name,n_nationkey
    from `/Root/nation` as n
    join $r as r on
    n.n_regionkey = r.r_regionkey);

$j2 = (select s_acctbal,s_name,s_address,s_phone,s_comment,n_name,s_suppkey
    from `/Root/supplier` as s
    join $j1 as j on
    s.s_nationkey = j.n_nationkey
);

$j3 = (select ps_partkey,ps_supplycost,s_acctbal,s_name,s_address,s_phone,s_comment,n_name
    from `/Root/partsupp` as ps
    join $j2 as j on
    ps.ps_suppkey = j.s_suppkey
);

$min_ps_supplycost = (select min(ps_supplycost) as min_ps_supplycost,ps_partkey
    from $j3
    group by ps_partkey
);

$p = (select p_partkey,p_mfgr
    from `/Root/part`
    where
    p_size = 10
    and p_type like '%COPPER'
);

$j4 = (select s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
    from $p as p
    join $j3 as j on p.p_partkey = j.ps_partkey
    join $min_ps_supplycost as m on p.p_partkey = m.ps_partkey
    where min_ps_supplycost=ps_supplycost
);

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from $j4
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
