-- Sort Spilling Test: Sort partsupp by multiple keys, compute running average
-- TPC-H scale 10000: ~80M rows in partsupp
-- Tests multi-key sort with window function on partsupp table.

$with_running = (
select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    avg(ps_supplycost) over w as running_avg_cost,
    sum(ps_availqty) over w as running_total_qty
from
    `column/tpch/s10000/partsupp`
window w as (order by ps_supplycost desc, ps_availqty asc, ps_partkey asc
             rows between unbounded preceding and current row)
);

-- Get statistics at regular intervals
select
    ps_partkey,
    ps_suppkey,
    ps_supplycost,
    ps_availqty,
    running_avg_cost,
    running_total_qty
from $with_running
where ps_partkey % 100000 = 0
order by ps_supplycost desc
limit 200;
