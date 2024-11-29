SELECT
    *
FROM (
    SELECT
        key,
        subkey,
        '' AS value
    FROM plato.Input
    UNION ALL
    SELECT
        *
    FROM plato.Input
    UNION ALL
    SELECT
        '' AS key,
        subkey,
        value
    FROM plato.Input
)
ORDER BY
    key,
    subkey,
    value;
