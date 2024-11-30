/* syntax version 1 */
/* postgres can not */
SELECT
    count(*) AS count,
    mod_sk + mod_k AS mod_sum
FROM plato.Input
    AS a
GROUP BY
    CAST(subkey AS uint32) % 10 AS mod_sk,
    CAST(key AS uint32) % 10 AS mod_k
ORDER BY
    count,
    mod_sum;
