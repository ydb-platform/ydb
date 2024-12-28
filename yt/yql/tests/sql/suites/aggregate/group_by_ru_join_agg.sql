/* syntax version 1 */
/* postgres can not */

pragma sampleselect;
select kk, sk, sum(cast(t2.subkey as Uint32)) FROM plato.Input AS t1 INNER JOIN plato.Input AS t2 USING (key) GROUP BY ROLLUP(t1.key as kk, t1.subkey as sk) ORDER BY kk, sk;
