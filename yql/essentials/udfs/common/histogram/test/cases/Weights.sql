/* syntax version 1 */
SELECT
    HISTOGRAM(key, value / subkey) AS basic_weight,
    HISTOGRAM(key, value, 3) AS weight_and_bins,
    Histogram::Print(HISTOGRAM(key)) <> Histogram::Print(HISTOGRAM(key, value)) AS equality_check
FROM (
    SELECT
        CAST(key AS Double) AS key,
        COALESCE(CAST(subkey AS Double), 1.0) AS subkey,
        COALESCE(CAST(value AS Double), 1.0) AS value
    FROM Input
);
