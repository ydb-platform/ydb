PRAGMA yt.UsePartitionsByKeysForFinalAgg = "false";

USE plato;

SELECT
    key,
    min(AsTuple(subkey, value)) AS min,
    max(AsTuple(subkey, value)) AS max,
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;

SELECT
    min(AsTuple(subkey, value)) AS min,
    max(AsTuple(subkey, value)) AS max,
FROM
    Input
;
