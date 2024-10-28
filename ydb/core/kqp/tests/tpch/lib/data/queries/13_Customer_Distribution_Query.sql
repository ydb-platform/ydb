-- $ID$
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$orders = (
    select
        o_orderkey,
        o_custkey
    from
        `$DBROOT$/orders`
    where
        o_comment NOT LIKE "%pending%accounts%"
);

select
    c_count,
    count(*) as custdist
from
    (
        select
            c.c_custkey as c_custkey,
            count(o.o_orderkey) as c_count
        from
            `$DBROOT$/customer` as c left outer join $orders as o on c.c_custkey = o.o_custkey
        group by
            c.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
