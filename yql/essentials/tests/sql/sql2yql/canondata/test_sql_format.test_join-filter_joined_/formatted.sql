PRAGMA DisableSimpleColumns;
USE plato;

SELECT
    b.key
FROM
    Input2 AS a
RIGHT JOIN
    Input3 AS b
ON
    a.key == b.key
WHERE
    a.key IS NULL
ORDER BY
    b.key
;
