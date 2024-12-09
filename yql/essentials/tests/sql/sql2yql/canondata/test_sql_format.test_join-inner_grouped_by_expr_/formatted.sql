PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    keyz,
    max(Input3.value) AS value
FROM plato.Input1
INNER JOIN plato.Input3
USING (key)
GROUP BY
    Input1.key AS keyz
ORDER BY
    keyz;
