/* postgres can not */
SELECT
    CAST(key AS int) AS key,
    subkey,
    value
FROM plato.Input
ORDER BY
    key,
    subkey;
