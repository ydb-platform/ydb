/* postgres can not */
FROM
    plato.Input
SELECT
    *
ORDER BY
    key,
    subkey
LIMIT 1;
