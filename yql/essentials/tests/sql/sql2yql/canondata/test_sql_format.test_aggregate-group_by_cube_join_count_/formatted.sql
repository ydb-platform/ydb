/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;

SELECT
    kk,
    sk,
    grouping(kk, sk),
    count(1)
FROM plato.Input
    AS t1
INNER JOIN plato.Input
    AS t2
USING (key)
GROUP BY
    CUBE (t1.key AS kk, t1.subkey AS sk)
ORDER BY
    kk,
    sk;
