PRAGMA DisableSimpleColumns;
USE plato;

SELECT
    b.*,
    1 AS q,
    a.*
FROM
    Input2 AS a
JOIN
    Input3 AS b
ON
    a.value == b.value
;
