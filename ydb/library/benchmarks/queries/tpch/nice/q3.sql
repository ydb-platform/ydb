{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{lineitem}}.l_orderkey as l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    {{orders}}.o_orderdate as o_orderdate,
    {{orders}}.o_shippriority as o_shippriority
from
    {{customer}}
    cross join {{orders}}
    cross join {{lineitem}}
where
    c_mktsegment = 'MACHINERY'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date('1995-03-08')
    and l_shipdate > date('1995-03-08')
group by
    {{lineitem}}.l_orderkey,
    {{orders}}.o_orderdate,
    {{orders}}.o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
