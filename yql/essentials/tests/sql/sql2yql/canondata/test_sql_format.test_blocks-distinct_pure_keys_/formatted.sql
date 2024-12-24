PRAGMA yt.UsePartitionsByKeysForFinalAgg = 'false';

USE plato;

SELECT
    key,
    count(DISTINCT subkey),
    sum(DISTINCT subkey),
    count(DISTINCT Unwrap(subkey / 2u)),
    sum(DISTINCT Unwrap(subkey / 2u))
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
