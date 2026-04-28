-- Sort Spilling Test: Join lineitem with orders, sort, compute window aggregate
-- TPC-H scale 10000: join produces ~100M+ rows (filtered by date range)
-- Tests sort spilling after a large join.

$joined = (
select
    l.l_orderkey as l_orderkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_shipdate as l_shipdate,
    o.o_orderdate as o_orderdate,
    o.o_totalprice as o_totalprice,
    o.o_orderpriority as o_orderpriority,
    sum(l.l_extendedprice * (CAST(1 AS Decimal(12,2)) - l.l_discount)) over w as running_revenue
from
    `column/tpch/s10000/lineitem` as l
join
    `column/tpch/s10000/orders` as o
on
    l.l_orderkey = o.o_orderkey
where
    l.l_shipdate >= Date('1995-01-01')
    and l.l_shipdate < Date('1996-01-01')
window w as (order by o.o_totalprice desc, l.l_shipdate asc
             rows between unbounded preceding and current row)
);

select
    o_orderpriority,
    count(*) as cnt,
    sum(l_extendedprice) as total_price,
    max(running_revenue) as max_running_revenue
from $joined
group by o_orderpriority
order by o_orderpriority;
