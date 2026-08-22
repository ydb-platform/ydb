PRAGMA UseBlocks;
PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
PRAGMA ydb.OptimizerHints = '
    JoinOrder((c n) o)
    JoinType(c n broadcast)
';

select c.c_custkey, o.o_orderkey, n.n_name
from customer c
join nation n on c.c_nationkey = n.n_nationkey
join orders o on c.c_custkey = o.o_custkey
