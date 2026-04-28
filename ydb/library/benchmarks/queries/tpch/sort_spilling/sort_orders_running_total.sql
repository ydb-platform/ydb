-- Sort Spilling Test: Sort orders by orderdate, compute running total
-- TPC-H scale 10000: ~150M rows in orders
-- Window function forces full sort by orderdate.

$with_running = (
select
    o_orderdate,
    o_totalprice,
    o_orderstatus,
    sum(o_totalprice) over w as running_total
from
    `{path}orders`
window w as (order by o_orderdate rows between unbounded preceding and current row)
);

-- Aggregate by date to get daily running totals
select
    o_orderdate,
    count(*) as order_count,
    sum(o_totalprice) as daily_total,
    max(running_total) as cumulative_total
from $with_running
group by o_orderdate
order by o_orderdate
limit 100;
