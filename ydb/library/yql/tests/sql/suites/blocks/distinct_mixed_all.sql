pragma UseBlocks;
pragma EmitAggApply;
pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

SELECT
    count(*),
    sum(distinct key),min(distinct key)
FROM Input
