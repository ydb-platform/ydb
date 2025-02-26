-- PRAGMA TablePathPrefix='olap-testing-sas-common/kikimr/pavelvelikhov/tpc/dt64/column/tpch/s1000';
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


-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            Substring(CAST(c_phone as String), 0u, 2u) as cntrycode,
            c_acctbal
        from
            customer
            cross join (
                select
                    avg(c_acctbal) as avg_acctbal
                from
                    customer
                where
                    c_acctbal > 0.00
                    and Substring(CAST (c_phone as String), 0u, 2u) in
                        ('31', '29', '30', '26', '28', '25', '15')
            ) avg_customer
            left only join 
                orders
            on orders.o_custkey = customer.c_custkey
        where
            Substring(CAST (c_phone as String), 0u, 2u) in
                ('31', '29', '30', '26', '28', '25', '15')
            and c_acctbal > avg_acctbal
    ) as custsale
group by
    cntrycode
order by
    cntrycode;