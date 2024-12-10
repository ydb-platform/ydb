/* postgres can not */
SELECT
    median(val) AS med,
    median(DISTINCT val) AS distinct_med,
    percentile(val, 0.8) AS p80
FROM (
    SELECT
        key,
        CAST(value AS int) AS val
    FROM
        plato.Input
)
GROUP BY
    key
ORDER BY
    med
;
