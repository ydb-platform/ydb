{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{customer}}.c_name as c_name,
    {{customer}}.c_custkey as c_custkey,
    {{orders}}.o_orderkey as o_orderkey,
    {{orders}}.o_orderdate as o_orderdate,
    {{orders}}.o_totalprice as o_totalprice,
    sum(l_quantity)
from
    {{customer}}
    cross join {{orders}}
    cross join {{lineitem}}
    cross join (
        select
            l_orderkey
        from
            {{lineitem}}
        group by
            l_orderkey having
                sum(l_quantity) > 315
    ) sum_l_quantity
where
    o_orderkey = sum_l_quantity.l_orderkey
    and c_custkey = o_custkey
    and o_orderkey = {{lineitem}}.l_orderkey
group by
    {{customer}}.c_name,
    {{customer}}.c_custkey,
    {{orders}}.o_orderkey,
    {{orders}}.o_orderdate,
    {{orders}}.o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
