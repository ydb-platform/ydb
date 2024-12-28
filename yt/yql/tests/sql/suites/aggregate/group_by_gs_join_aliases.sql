/* syntax version 1 */
/* postgres can not */

SELECT k1, k2, b.subkey as kk2, SOME(a.value) as val
FROM plato.Input AS a JOIN plato.Input AS b USING(key)
GROUP BY GROUPING SETS(
  (a.key as k1, b.subkey as k2),
  (k1),
  (b.subkey)
)
ORDER BY k1, kk2;

