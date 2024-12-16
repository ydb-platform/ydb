USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        value
    FROM
        Input
    UNION ALL
    SELECT
        subkey AS key,
        value
    FROM
        Input2
)
ORDER BY
    key,
    value
;
