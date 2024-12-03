/* syntax version 1 */
/* postgres can not */
SELECT
    count(1),
    key,
    subkey,
    value,
    grouping(key, subkey, value)
FROM plato.Input
GROUP BY
    GROUPING SETS (
        (),
        value,
        ROLLUP (key, subkey),
        ())
ORDER BY
    key,
    subkey,
    value;
