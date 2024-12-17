PRAGMA yt.UsePartitionsByKeysForFinalAgg = 'false';

USE plato;

SELECT
    count(*),
    max(subkey),
    sum(DISTINCT subkey),
    avg(subkey),
    count(DISTINCT subkey / 2u),
    avg(DISTINCT subkey / 2u)
FROM
    Input
;
