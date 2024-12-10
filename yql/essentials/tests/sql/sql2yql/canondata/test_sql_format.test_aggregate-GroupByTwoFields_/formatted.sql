/* syntax version 1 */
SELECT
    sum(c) AS sum_c,
    max(d) AS max_d
FROM
    plato.Input
GROUP BY
    a,
    b
ORDER BY
    sum_c,
    max_d
;
