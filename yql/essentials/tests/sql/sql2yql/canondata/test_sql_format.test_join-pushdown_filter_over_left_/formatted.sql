/* postgres can not */
PRAGMA DisableSimpleColumns;
USE plato;

-- should pushdown
SELECT
    *
FROM
    Input1 AS a
LEFT JOIN
    Input2 AS b
ON
    a.key == b.key
WHERE
    Unwrap(CAST(a.key AS Int32)) > 100
ORDER BY
    a.key
;
