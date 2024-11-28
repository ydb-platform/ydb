PRAGMA DisableSimpleColumns;

/* postgres can not */
FROM plato.Input1
INNER JOIN plato.Input3
USING (key)
SELECT
    Input1.key,
    Input1.subkey,
    Input3.value;
