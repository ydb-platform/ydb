/* syntax version 1 */
SELECT
    LogarithmicHistogram(value) AS default,
    LogHistogram(value, 2) AS log_size,
    LogHistogram(value, 0.5, 10, 10000) AS log_min_max
FROM (
    SELECT
        CAST(value AS Double) AS value
    FROM Input
);
