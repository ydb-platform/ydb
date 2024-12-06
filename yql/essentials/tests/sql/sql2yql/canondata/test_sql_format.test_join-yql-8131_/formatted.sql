/* kikimr can not */
USE plato;
PRAGMA DisableSimpleColumns;
PRAGMA yt.JoinCollectColumnarStatistics = "async";
PRAGMA yt.MinTempAvgChunkSize = "0";
PRAGMA yt.MapJoinLimit = "1";

SELECT
    *
FROM (
    SELECT DISTINCT
        key,
        subkey
    FROM
        Input
    WHERE
        CAST(key AS Int32) > 100
    ORDER BY
        key
    LIMIT 100
) AS a
RIGHT JOIN (
    SELECT
        key,
        value
    FROM
        Input
    WHERE
        CAST(key AS Int32) < 500
) AS b
USING (key)
ORDER BY
    a.key,
    a.subkey,
    b.value
;
