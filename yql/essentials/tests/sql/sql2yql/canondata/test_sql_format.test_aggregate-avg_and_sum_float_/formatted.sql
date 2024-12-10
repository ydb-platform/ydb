/* syntax version 1 */
SELECT
    key,
    avg(CAST(subkey AS Float)) AS avg,
    sum(CAST(subkey AS Float)) AS sum,
FROM
    plato.Input
GROUP BY
    key
ORDER BY
    key
;
