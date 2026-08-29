PRAGMA TablePathPrefix='/Root/';
PRAGMA ydb.OptDisallowFuseJoins='true';
PRAGMA ydb.UseGraceJoinCoreForMap = 'true';
PRAGMA ydb.OptEnableOlapPushdown='false';
pragma warning("disable", "4527");

PRAGMA AnsiImplicitCrossJoin;

PRAGMA ydb.OptimizerHints =
'
    JoinOrder[{ps s} p]
';

-- PRAGMA ydb.OptimizerHints =
-- '
--     JoinOrder(
--         (
--             (
--                 (
--                     (ps p)
--                     s
--                 )
--                 l
--             )
--             o
--         )
--         n
--     )
-- ';

-- PRAGMA ydb.OptimizerHints =
-- '
--     JoinOrder(
--         (
--             (
--                 (ps p)
--                 l
--             )
--             (s n)
--         )
--         o
--     )
-- ';

$z0 = cast(0 as decimal(7,2));
$z1_2 = cast("1.2" as decimal(7,2));
$z1_3 = cast("1.3" as decimal(7,2));
$z0_9 = cast("0.9" as decimal(7,2));
$z0_99 = cast("0.99" as decimal(7,2));
$z1_49 = cast("1.49" as decimal(7,2));

$z0_35_9 = cast(0 as decimal(35,9));
$z0_35 = cast(0 as decimal(35,2));
$z0_1_35 = cast("0.1" as decimal(35,2));
$z1_2_35 = cast("1.2" as decimal(35,2));
$z0_05_35 = cast("0.05" as decimal(35,2));
$z0_9_35 = cast("0.9" as decimal(35,2));
$z1_1_35 = cast("1.1" as decimal(35,2));
$z0_5_35 = cast("0.5" as decimal(35,2));
$z100_35 = cast(100 as decimal(35,2));
$z7_35 = cast("7." as decimal(35,2));

$z0_12 = cast(0 as decimal(12,2));
$z1_12 = cast(1 as decimal(12,2));
$z0_0100001_12 = cast("0.0100001" as decimal(12,2));
$z0_06_12 = cast("0.06" as decimal(12,2));
$z0_2_12 = cast("0.2" as decimal(12,2));

$scale_factor = 1;

$round = ($x,$y) -> {return $x;};
$upscale = ($x) -> { return cast($x as decimal(35,9)); };
$todecimal = ($x, $p, $s) -> { return cast(cast($x as string?) as decimal($p,$s)); };


-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Approved February 1998
-- using 1680793381 as a seed to the RNG

$j5 = (select n_name, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    part as p,
    partsupp as ps,
    lineitem as l,
    supplier as s,
    orders as o,
    nation as n

where p_name like '%green%'
AND ps_partkey = p_partkey
AND l_suppkey = ps_suppkey AND l_partkey = ps_partkey
AND l_suppkey = s_suppkey
AND o_orderkey = l_orderkey
AND s_nationkey = n_nationkey
);

$profit = (
  SELECT
    n_name AS nation,
    DateTime::GetYear(CAST(o_orderdate AS Timestamp)) AS o_year,
    CAST(l_extendedprice AS Double) *
      (
        1.0 - COALESCE(l_discount, 0.0)
      )
      - CAST(ps_supplycost AS Double) * CAST(l_quantity AS Double) AS amount
  FROM $j5
);

select
    nation,
    o_year,
    sum(amount) as sum_profit
from $profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;




