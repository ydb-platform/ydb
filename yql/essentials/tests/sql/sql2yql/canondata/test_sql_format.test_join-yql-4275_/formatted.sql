PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT
    *
FROM
    Input AS x1
JOIN (
    SELECT
        key ?? 4 AS key
    FROM
        Input
) AS x2
ON
    x1.key == x2.key
WHERE
    x2.key == 4
;
