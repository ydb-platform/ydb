-- Sort Spilling Test: Sort lineitem by shipdate, compute running sum
-- TPC-H scale 10000: ~600M rows in lineitem
-- The window function forces a full WideSort that cannot be replaced with TopSort.
-- After sorting, we aggregate the windowed results to produce a small output.

$sorted = (
select
    l_shipdate,
    l_quantity,
    l_extendedprice,
    sum(l_quantity) over w as running_qty,
    sum(l_extendedprice) over w as running_price
from
    `column/tpch/s10000/lineitem`
window w as (order by l_shipdate rows between unbounded preceding and current row)
);

select
    l_shipdate,
    max(running_qty) as max_running_qty,
    max(running_price) as max_running_price,
    count(*) as cnt
from $sorted
group by l_shipdate
order by l_shipdate
limit 100;
