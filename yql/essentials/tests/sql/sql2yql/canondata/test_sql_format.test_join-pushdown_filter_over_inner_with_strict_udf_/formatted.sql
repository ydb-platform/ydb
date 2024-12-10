/* postgres can not */
PRAGMA DisableSimpleColumns;

USE plato;

-- should pushdown
SELECT
    *
FROM
    Input1 AS a
INNER JOIN
    Input2 AS b
ON
    a.key == b.key
WHERE
    Math::IsFinite(CAST(a.key AS Double))
ORDER BY
    a.key
;
