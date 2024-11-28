/* syntax version 1 *//* postgres can not */
SELECT
    sum(length(value)),
    key,
    subkey
FROM plato.Input
GROUP BY
    CUBE (key, subkey)
ORDER BY
    key,
    subkey;
