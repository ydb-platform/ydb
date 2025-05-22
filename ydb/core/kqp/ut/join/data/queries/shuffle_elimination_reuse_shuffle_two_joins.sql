PRAGMA UseBlocks;
PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
PRAGMA ydb.OptimizerHints = 
'
    JoinType(ps p Shuffle)
    JoinType(ps p s Shuffle)
    JoinOrder((ps p) s) 
';

SELECT
    p.p_partkey
FROM
    supplier as s
JOIN
    partsupp as ps ON s.s_nationkey = ps.ps_partkey
JOIN
    part as p ON p.p_partkey = ps.ps_partkey;
