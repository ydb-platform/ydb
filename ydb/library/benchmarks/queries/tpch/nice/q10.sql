{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{customer}}.c_custkey as c_custkey,
    {{customer}}.c_name as c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    {{customer}}.c_acctbal as c_acctbal,
    {{nation}}.n_name as n_name,
    {{customer}}.c_address as c_address,
    {{customer}}.c_phone as c_phone,
    {{customer}}.c_comment as c_comment
from
    {{customer}}
    cross join {{orders}}
    cross join {{lineitem}}
    cross join {{nation}}
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date('1993-12-01')
    and o_orderdate < date('1993-12-01') + interval('P90D')
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    {{customer}}.c_custkey,
    {{customer}}.c_name,
    {{customer}}.c_acctbal,
    {{customer}}.c_phone,
    {{nation}}.n_name,
    {{customer}}.c_address,
    {{customer}}.c_comment
order by
    revenue desc
limit 20;


