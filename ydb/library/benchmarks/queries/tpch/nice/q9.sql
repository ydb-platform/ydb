{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            DateTime::GetYear(o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            {{part}}
            cross join {{supplier}}
            cross join {{lineitem}}
            cross join {{partsupp}}
            cross join {{orders}}
            cross join {{nation}}
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%rose%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
