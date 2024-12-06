/* syntax version 1 */
/* postgres can not */
--INSERT INTO Output
SELECT
    a.key AS kk,

    --    key as kkk,
    aggregate_list(subkey)
FROM plato.Input4
    AS a
GROUP BY
    a.key AS kk
ORDER BY
    kk;
