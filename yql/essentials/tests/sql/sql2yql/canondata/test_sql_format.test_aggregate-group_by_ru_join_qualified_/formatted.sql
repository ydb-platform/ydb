/* syntax version 1 */
/* postgres can not */
SELECT
    k,
    b.subkey AS sk,
    MIN(a.value) AS val,
    GROUPING(k, b.subkey) AS g,
FROM plato.Input
    AS a
JOIN plato.Input
    AS b
USING (key)
GROUP BY
    ROLLUP (a.key AS k, b.subkey)
ORDER BY
    g,
    k,
    sk;
