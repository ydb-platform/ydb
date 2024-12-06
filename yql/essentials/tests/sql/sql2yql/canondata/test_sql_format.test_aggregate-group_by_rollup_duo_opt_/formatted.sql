/* syntax version 1 */
/* postgres can not */
SELECT
    count(1),
    k,
    subkey
FROM plato.Input
GROUP BY
    ROLLUP (CAST(key AS uint32) AS k, subkey)
ORDER BY
    k,
    subkey;
