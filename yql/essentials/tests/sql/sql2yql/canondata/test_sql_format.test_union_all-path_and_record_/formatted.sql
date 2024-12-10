/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        value,
        TablePath() AS path,
        TableRecordIndex() AS record
    FROM
        Input
    UNION ALL
    SELECT
        key,
        value,
        "" AS path,
        TableRecordIndex() AS record
    FROM
        Input
)
ORDER BY
    key,
    path,
    record
;
