PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;
PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";

SELECT
    n_name,
    Sum(l_extendedprice * (1.0 - l_discount)) AS revenue
FROM
    `/Root/customer` AS customer,
    `/Root/orders` AS orders,
    `/Root/lineitem` AS lineitem,
    `/Root/supplier` AS supplier,
    `/Root/nation` AS nation,
    `/Root/region` AS region
WHERE
    c_custkey == o_custkey
    AND l_orderkey == o_orderkey
    AND l_suppkey == s_suppkey
    AND c_nationkey == s_nationkey
    AND s_nationkey == n_nationkey
    AND n_regionkey == r_regionkey
    AND r_name == 'ASIA'
    AND o_orderdate >= Date('1994-01-01')
    AND o_orderdate < DateTime::MakeDate(DateTime::ShiftYears(Date('1994-01-01'), 1))
GROUP BY
    n_name
ORDER BY
    revenue DESC
;
