/* syntax version 1 */
/* postgres can not */
--INSERT INTO Output
SELECT
    a.key as kk,
--    key as kkk,
    aggregate_list(subkey)
FROM plato.Input4 as a
GROUP BY a.key as kk
ORDER BY kk;
