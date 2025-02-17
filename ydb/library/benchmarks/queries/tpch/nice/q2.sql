{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    {{part}}
    cross join {{supplier}}
    cross join {{partsupp}}
    cross join {{nation}}
    cross join {{region}}
    cross join (
        select
            {{partsupp}}.ps_partkey as sc_ps_partkey,
            min(ps_supplycost) as min_ps_supplycost
        from
            {{partsupp}}
            cross join {{supplier}}
            cross join {{nation}}
            cross join {{region}}
        where
            s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'AMERICA'
        group by {{partsupp}}.ps_partkey
            ) as min_ps_supplycosts
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 10
    and p_type like '%COPPER'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AMERICA'
    and ps_supplycost = min_ps_supplycost
    and p_partkey = ps_partkey
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
