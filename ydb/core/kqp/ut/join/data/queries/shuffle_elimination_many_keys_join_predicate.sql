PRAGMA ydb.OptShuffleElimination = 'true';
PRAGMA ydb.CostBasedOptimizationLevel='3';

PRAGMA ydb.OptimizerHints = 
'
    JoinType(ps l Shuffle)
    JoinOrder(ps l) 
';


select
    ps_suppkey
from
    partsupp ps
    cross join lineitem l
where
    ps.ps_partkey = l.l_partkey and ps.ps_suppkey = l.l_suppkey
   