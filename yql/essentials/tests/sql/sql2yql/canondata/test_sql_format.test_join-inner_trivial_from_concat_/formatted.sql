PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    A.key,
    A.subkey,
    B.value
FROM
    plato.concat(Input1, Input2) AS A
INNER JOIN
    plato.Input3 AS B
USING (key);
