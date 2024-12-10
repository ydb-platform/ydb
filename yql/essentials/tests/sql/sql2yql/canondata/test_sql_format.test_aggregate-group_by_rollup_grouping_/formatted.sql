/* syntax version 1 */
/* postgres can not */
SELECT
    sum(length(value)) AS s,
    m0,
    m1,
    m2,
    grouping(m0, m1, m2) AS ggg
FROM
    plato.Input
GROUP BY
    ROLLUP (CAST(key AS uint32) AS m0, CAST(key AS uint32) % 10u AS m1, CAST(key AS uint32) % 100u AS m2)
ORDER BY
    s,
    m0,
    m1,
    m2
;
