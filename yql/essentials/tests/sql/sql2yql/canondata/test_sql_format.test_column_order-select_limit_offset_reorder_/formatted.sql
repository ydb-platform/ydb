/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

SELECT
    subkey,
    key,
    value
FROM (
    SELECT
        *
    FROM
        Input
) AS x
ORDER BY
    key,
    subkey
LIMIT 1 OFFSET 1;
