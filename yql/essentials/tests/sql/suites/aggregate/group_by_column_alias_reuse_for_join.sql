/* syntax version 1 */
/* postgres can not */
--INSERT INTO Output
SELECT
    a.key as kk,
--    kk,
    aggregate_list(b.subkey)
FROM plato.Input as a
JOIN plato.Input4 as b
ON a.subkey == b.key
GROUP BY a.key as kk
ORDER BY kk;
