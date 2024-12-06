/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.DisableOptimizers = "HorizontalJoin,MultiHorizontalJoin";

SELECT
    *
FROM (
    SELECT
        key,
        TableRecordIndex() AS record,
        TablePath() AS path
    FROM
        Input
    UNION ALL
    SELECT
        key,
        TableRecordIndex() AS record,
        "d" AS path
    FROM
        Input
) AS x
ORDER BY
    key,
    record,
    path
;
