PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    *
FROM
    plato.Input1 AS A
INNER JOIN
    plato.Input2 AS B
ON
    A.key == B.key
;
