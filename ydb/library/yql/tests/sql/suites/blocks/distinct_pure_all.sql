pragma UseBlocks;
pragma EmitAggApply;
pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

SELECT
    sum(distinct key),min(distinct key)
FROM Input
