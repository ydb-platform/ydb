SELECT
    *
FROM (
    SELECT
        CAST(key AS int) AS key,
        '' AS subkey,
        '' AS value
    FROM plato.Input
    UNION ALL
    SELECT
        1 AS key,
        subkey,
        '' AS value
    FROM plato.Input
    UNION ALL
    SELECT
        1 AS key,
        '' AS subkey,
        value
    FROM plato.Input
)
ORDER BY
    key,
    subkey,
    value;
