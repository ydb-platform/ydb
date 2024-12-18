PRAGMA DisableSimpleColumns;

SELECT
    Input1.key AS key,
    Input1.subkey,
    Input3.value
FROM
    plato.Input1
INNER JOIN
    plato.Input3
USING (key)
ORDER BY
    key DESC
;
