PRAGMA yt.UsePartitionsByKeysForFinalAgg = 'false';

USE plato;

SELECT
    sum(DISTINCT key),
    min(DISTINCT key)
FROM
    Input
;
