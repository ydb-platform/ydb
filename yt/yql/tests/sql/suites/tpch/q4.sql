
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-03-01");

$join = (select
        o.o_orderpriority as o_orderpriority,
        o.o_orderdate as o_orderdate,
        l.l_commitdate as l_commitdate,
        l.l_receiptdate as l_receiptdate
    from
        plato.orders as o
        join any plato.lineitem as l
        on o.o_orderkey = l.l_orderkey);

select
    o_orderpriority,
    count(*) as order_count
from $join
where
    CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < DateTime::MakeDate(DateTime::ShiftMonths($border, 3))
    and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority;
