-- Sort Spilling Test: Sort partsupp by multiple keys, assign row numbers
-- ~80M rows at scale 10000. Tests multi-key sort on partsupp table.

$numbered = (
select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    row_number() over (order by ps_supplycost desc, ps_availqty asc, ps_partkey asc) as rn
from
    `column/tpch/s10000/partsupp`
);

-- Sample to verify sort order
select
    rn,
    ps_partkey,
    ps_suppkey,
    ps_supplycost,
    ps_availqty
from $numbered
where rn % 800000 = 1
order by rn
limit 200;
