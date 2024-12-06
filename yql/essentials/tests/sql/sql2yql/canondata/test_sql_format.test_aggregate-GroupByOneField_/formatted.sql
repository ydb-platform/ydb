/* syntax version 1 */
SELECT
    sum(c) AS sumc,
    max(d) AS maxd
FROM plato.Input
GROUP BY
    a
ORDER BY
    sumc,
    maxd;
