/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

SELECT
    *
FROM
    Input
ORDER BY
    key
LIMIT 1 OFFSET 3;

SELECT
    *
FROM
    Input
ORDER BY
    value
LIMIT 0 OFFSET 3;

SELECT
    *
FROM
    Input
LIMIT 0;
