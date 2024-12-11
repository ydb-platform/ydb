PRAGMA DisableSimpleColumns;

SELECT
    Input1.key AS key,
    Input1.subkey,
    CAST(Input3.value AS varchar) AS value
FROM
    plato.Input1
LEFT JOIN
    plato.Input3
USING (key)
ORDER BY
    key
;
