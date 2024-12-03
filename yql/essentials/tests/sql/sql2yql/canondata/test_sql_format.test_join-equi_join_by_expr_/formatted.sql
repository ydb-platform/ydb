PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    *
FROM plato.Input2
    AS A
JOIN plato.Input3
    AS B
ON CAST(A.key AS INT) + 1 == CAST(B.key AS INT);
