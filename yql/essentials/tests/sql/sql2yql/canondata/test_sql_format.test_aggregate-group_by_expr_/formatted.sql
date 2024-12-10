/* syntax version 1 */
/* postgres can not */
SELECT
    sum(CAST(subkey AS uint32)) AS s
FROM
    plato.Input
GROUP BY
    CAST(key AS uint32) % 10
ORDER BY
    s
;
