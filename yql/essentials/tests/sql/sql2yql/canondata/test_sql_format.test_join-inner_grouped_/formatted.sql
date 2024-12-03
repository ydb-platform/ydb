PRAGMA DisableSimpleColumns;

SELECT
    Input1.key AS key,
    max(Input3.value) AS value
FROM plato.Input1
INNER JOIN plato.Input3
USING (key)
GROUP BY
    Input1.key
ORDER BY
    key;
