/* syntax version 1 */
/* postgres can not */
pragma sampleselect;
select kk, sk, grouping(kk, sk),count(1) FROM plato.Input AS t1 INNER JOIN plato.Input AS t2 USING (key)
GROUP BY CUBE(t1.key as kk, t1.subkey as sk) ORDER BY kk, sk;
