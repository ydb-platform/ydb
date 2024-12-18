PRAGMA yt.UsePartitionsByKeysForFinalAgg = 'false';

USE plato;

SELECT
    count(*),
    sum(DISTINCT key),
    min(DISTINCT key)
FROM
    Input
;
