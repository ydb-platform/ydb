PRAGMA TablePathPrefix='/Root';

-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    nation.n_name as n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer
    cross join orders
    cross join lineitem
    cross join supplier
    cross join nation
    cross join region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AFRICA'
    and o_orderdate >= date('1995-01-01')
    and o_orderdate < date('1995-01-01') + interval('P365D')
group by
    nation.n_name
order by
    revenue desc;