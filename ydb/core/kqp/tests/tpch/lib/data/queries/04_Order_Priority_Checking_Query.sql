-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1993-01-01");

$join = (
    select
        o.o_orderpriority as o_orderpriority,
        o.o_orderdate as o_orderdate,
        l.l_commitdate as l_commitdate,
        l.l_receiptdate as l_receiptdate
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/orders` as o on o.o_orderkey = l.l_orderkey
);

select
    o_orderpriority,
    count(*) as order_count
from
    $join
where
    CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval("P93D"))
    and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority;
