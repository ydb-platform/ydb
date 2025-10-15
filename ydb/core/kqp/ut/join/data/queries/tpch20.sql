PRAGMA ydb.OptShuffleElimination = 'true';
PRAGMA ydb.CostBasedOptimizationLevel='3';
pragma warning("disable", "4527");

$z0 = 0;
$z1_2 = 1.2;
$z1_3 = 1.3;
$z0_9 = 0.9;
$z0_99 = 0.99;
$z1_49 = 1.49;

$z0_35 = 0;
$z0_1_35 = 0.1;
$z1_2_35 = 1.2;
$z0_05_35 = 0.05;
$z0_9_35 = 0.9;
$z1_1_35 = 1.1;
$z0_5_35 = 0.5;
$z100_35 = 100.;
$z0_0001_35 = 0.0001;
$z7_35 = 7.;

$z0_12 = 0.;
$z1_12 = 1;
$z0_0100001_12 = 0.0100001;
$z0_06_12 = 0.06;
$z0_2_12 = 0.2;

$scale_factor = 1;

$round = ($x, $y) -> { return Math::Round($x, $y); };
$upscale = ($x) -> { return $x; };

$todecimal = ($x, $p, $s) -> { return cast($x as double); };


-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    s_name,
    s_address
from
    supplier
    cross join nation
    cross join (
        select
            ps_suppkey
        from
            partsupp
            cross join part
            cross join (
                select
                    l_partkey,
                    l_suppkey,
                    0.5 * sum(l_quantity) as q_threshold
                from
                    lineitem
                where
                    l_shipdate >= date('1993-01-01')
                    and l_shipdate < date('1993-01-01') + interval('P365D')
                group by
                    l_partkey,
                    l_suppkey
            ) as threshold
        where
            ps_partkey = p_partkey
            and ps_partkey = l_partkey
            and ps_suppkey = l_suppkey
            and p_name like 'maroon%'
            and ps_availqty > threshold.q_threshold
    ) as partsupp
where
    s_suppkey = ps_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'VIETNAM'
order by
    s_name;