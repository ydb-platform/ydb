/* syntax version 1 */
SELECT
    LinearHistogram(value) AS default,
    LinearHistogram(value, 33) AS linear_size,
    LinearHistogram(value, 100, 100, 1000) AS linear_min_max
FROM (
    SELECT
        CAST(value AS Double) AS value
    FROM Input
);
