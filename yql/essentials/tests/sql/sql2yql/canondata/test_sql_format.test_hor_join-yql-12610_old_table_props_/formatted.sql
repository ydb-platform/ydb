/* postgres can not */
/* syntax version 1 */
/* kikimr can not - yt pragma */
USE plato;
PRAGMA yt.UseSystemColumns = "0";

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
