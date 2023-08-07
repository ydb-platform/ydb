{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$orders = (
    select
        o_orderkey,
        o_custkey
    from
        {{orders}}
    where
        o_comment NOT LIKE "%unusual%requests%"
);
select
    c_count as c_count,
    count(*) as custdist
from
    (
        select
            c.c_custkey as c_custkey,
            count(o.o_orderkey) as c_count
        from
            {{customer}} as c left outer join $orders as o on
                c.c_custkey = o.o_custkey
        group by
            c.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
