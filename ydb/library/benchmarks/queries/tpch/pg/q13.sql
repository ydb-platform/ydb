{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    coalesce(c_count, 0) as c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            {{customer}} left outer join {{orders}} on
                c_custkey = o_custkey
                and o_comment not like '%unusual%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

