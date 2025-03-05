pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

SELECT
    key, count(*), count(distinct subkey), sum(distinct subkey), 
    count(distinct Unwrap(subkey/2u)), sum(distinct Unwrap(subkey/2u))
FROM Input
GROUP BY key
ORDER BY key
