PRAGMA yt.UsePartitionsByKeysForFinalAgg = "false";
USE plato;

SELECT
    key,
    count(*),
    count(subkey),
    min(subkey),
    max(subkey),
    sum(subkey),
    avg(subkey)
FROM Input
GROUP BY
    key
ORDER BY
    key;
