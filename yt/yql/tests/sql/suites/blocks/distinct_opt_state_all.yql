pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

SELECT
    count(*),max(subkey),sum(distinct subkey),avg(subkey),count(distinct subkey/2u),avg(distinct subkey/2u)
FROM Input
