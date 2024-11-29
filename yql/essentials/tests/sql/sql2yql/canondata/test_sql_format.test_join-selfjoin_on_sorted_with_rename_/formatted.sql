PRAGMA DisableSimpleColumns;

SELECT
    a.key AS key,
    b.subkey AS subkey,
    b.value AS value
FROM plato.Input
    AS a
INNER JOIN plato.Input
    AS b
ON a.key == b.key;
