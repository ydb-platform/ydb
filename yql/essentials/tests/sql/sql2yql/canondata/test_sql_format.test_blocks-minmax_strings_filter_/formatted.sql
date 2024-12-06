PRAGMA yt.UsePartitionsByKeysForFinalAgg = "false";
USE plato;

SELECT
    key,
    max(s) AS maxs,
    min(s) AS mins,
    min(s_opt) AS mins_opt,
    max(s_opt) AS maxs_opt,
FROM
    Input
WHERE
    key != "1" AND s NOT IN ("7", "8")
GROUP BY
    key
ORDER BY
    key
;
