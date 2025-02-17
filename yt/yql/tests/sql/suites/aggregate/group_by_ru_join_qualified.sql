/* syntax version 1 */
/* postgres can not */

SELECT
    k,
    b.subkey as sk,
    MIN(a.value) as val,
    GROUPING(k, b.subkey) as g,
FROM plato.Input AS a JOIN plato.Input AS b USING(key)
GROUP BY ROLLUP (a.key as k, b.subkey)
ORDER BY g, k, sk
;
