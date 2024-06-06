{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    c_count,
    count(*) as custdist
from
    (
        select
            {{customer}}.c_custkey as c_custkey,
            count(o_orderkey) as c_count
        from
            {{customer}} left outer join (
                select
                    o_orderkey, o_custkey
                from
                    {{orders}}
                where
                    o_comment not like '%unusual%requests%') as orders
            on
                {{customer}}.c_custkey = orders.o_custkey

        group by
            {{customer}}.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;

