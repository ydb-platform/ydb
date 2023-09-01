/* syntax version 1 */
SELECT
    Histogram::Normalize(HISTOGRAM(key)) AS key_histogram,
    Histogram::Normalize(HISTOGRAM(subkey), 1.0) AS subkey_histogram,
    Histogram::Normalize(HISTOGRAM(value), -1.0) AS value_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
