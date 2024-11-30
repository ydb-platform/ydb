/* syntax version 1 */
/* postgres can not */
SELECT
    count(*) AS count,
    mod_sk
FROM plato.Input
    AS a
GROUP BY
    CAST(subkey AS uint32) % 10 AS mod_sk,
    CAST(key AS uint32) % 10 AS mod_k
HAVING mod_k == 7;
