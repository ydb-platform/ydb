PRAGMA UseBlocks;
PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
PRAGMA ydb.OptimizerHints = 
'
    JoinType(t1 t2 Shuffle)
    JoinType(t1 t2 t3 Shuffle)
    JoinOrder((t1 t2) t3) 
';

SELECT 
    t1.id1,
    t2.id2,
    t2.t1_id1,
    t3.id3
FROM 
    t1 AS t1
INNER JOIN 
    t2 AS t2 
ON t1.id1 = t2.t1_id1
INNER JOIN
    t3 AS t3
ON t1.id1 = t3.id3;
