/* postgres can not */
SELECT
    value,
    subkey,
    key
FROM (
    SELECT
        *
    FROM
        plato.Input
)
ORDER BY
    key,
    subkey
LIMIT 1, 2;
