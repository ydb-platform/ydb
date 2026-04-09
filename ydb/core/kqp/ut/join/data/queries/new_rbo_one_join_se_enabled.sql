PRAGMA YqlSelect = 'force';
PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
PRAGMA ydb.OptimizerHints =
'
    JoinType(c o Shuffle)
    JoinOrder(c o)
';

SELECT
    o.o_custkey
FROM
    `/Root/customer` AS c  JOIN
    `/Root/orders` AS o    ON o.o_custkey = c.c_custkey
WHERE
    c.c_name = "BAPBAPA";
