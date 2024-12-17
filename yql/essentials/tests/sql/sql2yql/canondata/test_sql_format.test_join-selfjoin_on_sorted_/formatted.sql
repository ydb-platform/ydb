PRAGMA DisableSimpleColumns;

SELECT
    *
FROM
    plato.Input AS a
INNER JOIN
    plato.Input AS b
ON
    a.key == b.key
;
