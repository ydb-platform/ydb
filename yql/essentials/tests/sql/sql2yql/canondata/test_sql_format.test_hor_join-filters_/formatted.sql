USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        value
    FROM plato.Input1
    WHERE key > "010"
    UNION ALL
    SELECT
        key,
        value
    FROM plato.Input2
    WHERE key > "020"
)
    AS x
ORDER BY
    key,
    value;
