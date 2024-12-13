SELECT
    median(val + 1) AS med,
    median(DISTINCT val + 1) AS distinct_med,
    percentile(val + 1, 0.8) AS p80
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
