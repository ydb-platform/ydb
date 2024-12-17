/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        TableRecordIndex() AS record,
        TablePath() AS path
    FROM
        Input1
    UNION ALL
    SELECT
        key,
        TableRecordIndex() AS record,
        'd' AS path
    FROM
        Input2
) AS x
ORDER BY
    key,
    record,
    path
;
