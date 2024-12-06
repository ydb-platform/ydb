/* postgres can not */
SELECT
    coalesce(CAST(key AS int), 0) AS key,
    subkey,
    value
FROM plato.Input
ORDER BY
    key DESC,
    subkey;
