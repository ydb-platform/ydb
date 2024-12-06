PRAGMA DisableSimpleColumns;
USE plato;

SELECT
    *
FROM
    Input2 AS a
LEFT JOIN
    Input3 AS b
ON
    a.value == b.value
;
