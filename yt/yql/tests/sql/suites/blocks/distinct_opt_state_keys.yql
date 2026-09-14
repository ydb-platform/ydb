pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

SELECT
    key,count(*),max(subkey),sum(distinct subkey),avg(subkey),count(distinct subkey/2u),avg(distinct subkey/2u)
FROM Input
GROUP BY key
ORDER BY key

