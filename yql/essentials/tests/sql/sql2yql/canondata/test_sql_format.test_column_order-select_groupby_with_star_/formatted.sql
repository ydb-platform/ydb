/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;

SELECT
    *
FROM
    Input
GROUP BY
    value,
    key
ORDER BY
    key,
    value
;

SELECT
    *
FROM
    Input
GROUP BY
    value,
    key
HAVING
    key == "150"
;

SELECT
    *
FROM
    Input
GROUP BY
    subkey,
    key || "x" AS key
ORDER BY
    subkey,
    key
;
