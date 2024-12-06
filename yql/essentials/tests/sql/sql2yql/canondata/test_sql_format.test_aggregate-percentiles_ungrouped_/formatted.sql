/* postgres can not */
SELECT
    median(val) AS med,
    percentile(val, 0.8) AS p80
FROM (
    SELECT
        CAST(value AS int) AS val
    FROM plato.Input
);
