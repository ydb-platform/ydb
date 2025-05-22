-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998

$PRAGMAS$

$in = (
    select
        l_orderkey,
        sum(l_quantity) as sum_l_quantity
    from
        `$DBROOT$/lineitem`
    group by
        l_orderkey having
            sum(l_quantity) > 250
);

$join1 = (
    select
        c.c_name as c_name,
        c.c_custkey as c_custkey,
        o.o_orderkey as o_orderkey,
        o.o_orderdate as o_orderdate,
        o.o_totalprice as o_totalprice
    from
        `$DBROOT$/orders` as o join `$DBROOT$/customer` as c on c.c_custkey = o.o_custkey
);

select
    j.c_name as c_name,
    j.c_custkey as c_custkey,
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.o_totalprice as o_totalprice,
    sum(i.sum_l_quantity) as sum_l_quantity
from
    $join1 as j join $in as i on i.l_orderkey = j.o_orderkey
group by
    j.c_name,
    j.c_custkey,
    j.o_orderkey,
    j.o_orderdate,
    j.o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
