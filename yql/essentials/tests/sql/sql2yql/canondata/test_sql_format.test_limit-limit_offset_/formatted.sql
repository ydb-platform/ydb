SELECT
    value,
    subkey,
    key
FROM (
    SELECT
        *
    FROM plato.Input
)
    AS x
ORDER BY
    key,
    subkey
LIMIT 1 OFFSET 1;
