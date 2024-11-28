PRAGMA DisableSimpleColumns;
/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    a.v,
    b.value
FROM `Input1`
    VIEW `ksv`
    AS a
JOIN `Input2`
    AS b
ON a.k == b.key
ORDER BY
    a.v;
