PRAGMA DisableSimpleColumns;

SELECT
    Input1.key AS key,
    Input1.subkey AS subkey,
    selected.value AS value
FROM
    plato.Input1
INNER JOIN (
    SELECT
        key,
        value || value AS value
    FROM
        plato.Input3
) AS selected
USING (key)
ORDER BY
    key DESC
;
