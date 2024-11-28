USE plato;

SELECT
    *
FROM (
    SELECT
        1 AS key,
        subkey,
        '' AS value
    FROM plato.Input1
    UNION ALL
    SELECT
        1 AS key,
        '' AS subkey,
        value
    FROM plato.Input2
    UNION ALL
    SELECT
        CAST(key AS Int32) AS key,
        '' AS subkey,
        value
    FROM plato.Input3
)
ORDER BY
    key,
    subkey,
    value;
