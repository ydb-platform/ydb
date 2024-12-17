/* syntax version 1 */
/* postgres can not */
SELECT
    *
FROM
    plato.Input AS t1
INNER JOIN
    plato.Input AS t2
USING (key)
GROUP BY
    ROLLUP (t1.key AS kk, t1.subkey)
ORDER BY
    kk,
    subkey
;
