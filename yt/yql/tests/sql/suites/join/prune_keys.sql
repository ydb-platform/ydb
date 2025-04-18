/* postgres can not */
use plato;

pragma yt.JoinMergeTablesLimit = "10";
pragma config.flags('OptimizerFlags', 'EmitPruneKeys');

-- PruneKeys
select *
from a_sorted
where v1 in (select v2 from b_sorted);

-- PruneAdjacentKeys
select *
from a_sorted
where k1 in (select k2 from b_sorted);
