-- $ID$
-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join = (
    select
        l.l_shipmode as l_shipmode,
        o.o_orderpriority as o_orderpriority,
        l.l_commitdate as l_commitdate,
        l.l_shipdate as l_shipdate,
        l.l_receiptdate as l_receiptdate
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/orders` as o on o.o_orderkey == l.l_orderkey
);

$border = Date("1996-01-01");

select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1
        else 0
    end) as low_line_count
from $join
where
    (l_shipmode = 'AIR' or l_shipmode = 'RAIL')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and cast(l_receiptdate as timestamp) >= $border
    and cast(l_receiptdate as timestamp) < ($border + Interval("P365D"))
group by
    l_shipmode
order by
    l_shipmode;
