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
        2 AS key,
        '' AS subkey,
        value
    FROM plato.Input2
    UNION ALL
    SELECT
        3 AS key,
        subkey,
        '' AS value
    FROM plato.Input3
    UNION ALL
    SELECT
        4 AS key,
        '' AS subkey,
        value
    FROM plato.Input4
)
    AS x
ORDER BY
    key,
    subkey,
    value;
