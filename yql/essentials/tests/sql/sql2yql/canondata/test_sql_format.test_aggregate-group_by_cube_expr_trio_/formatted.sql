/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;

SELECT
    sum(length(value)) AS s,
    m0,
    m1,
    m2
FROM plato.Input
GROUP BY
    ROLLUP (CAST(key AS uint32) AS m0, CAST(key AS uint32) % 10u AS m1, CAST(key AS uint32) % 100u AS m2)
ORDER BY
    s,
    m0,
    m1,
    m2;
