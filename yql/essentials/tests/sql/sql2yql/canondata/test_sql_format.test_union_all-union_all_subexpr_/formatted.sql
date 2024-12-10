SELECT
    *
FROM (
    SELECT
        key,
        CAST(subkey AS int) AS subkey,
        NULL AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        key,
        NULL AS subkey,
        value
    FROM
        plato.Input
) AS x
ORDER BY
    key,
    subkey,
    value
;
