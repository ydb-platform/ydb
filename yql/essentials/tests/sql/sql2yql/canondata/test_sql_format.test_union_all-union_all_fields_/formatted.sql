SELECT
    *
FROM (
    SELECT
        CAST(key AS int) AS key,
        '' AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        0 AS key,
        value
    FROM
        plato.Input
)
ORDER BY
    key,
    value
;
