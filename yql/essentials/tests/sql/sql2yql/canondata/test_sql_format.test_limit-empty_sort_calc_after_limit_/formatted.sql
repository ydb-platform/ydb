/* postgres can not */
SELECT
    *
FROM plato.Input
ORDER BY
    key || subkey
LIMIT 100 OFFSET 90;
