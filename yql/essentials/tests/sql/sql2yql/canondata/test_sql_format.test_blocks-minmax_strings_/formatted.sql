PRAGMA yt.UsePartitionsByKeysForFinalAgg = "false";
USE plato;

SELECT
    key,
    max(s) AS maxs,
    min(s) AS mins,
    min(s_opt) AS mins_opt,
    max(s_opt) AS maxs_opt,
    max(DISTINCT utf) AS dmaxs,
    min(DISTINCT utf) AS dmins,
    min(DISTINCT s_opt) AS dmins_opt,
    max(DISTINCT s_opt) AS dmaxs_opt,
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
