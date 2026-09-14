PRAGMA UseBlocks;
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
    customer c  JOIN 
    orders o    ON o.o_custkey = c.c_custkey
WHERE
    c.c_name = "BAPBAPA";