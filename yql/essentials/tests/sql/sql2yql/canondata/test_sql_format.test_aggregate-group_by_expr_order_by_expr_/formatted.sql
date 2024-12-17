/* syntax version 1 */
/* postgres can not */
SELECT
    sum(CAST(key AS uint32)) AS keysum
FROM
    plato.Input
GROUP BY
    CAST(key AS uint32) / 100 + CAST(subkey AS uint32) % 10
ORDER BY
    keysum DESC
;
