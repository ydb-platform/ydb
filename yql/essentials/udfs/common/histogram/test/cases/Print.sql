/* syntax version 1 */
SELECT
    Histogram::Print(HISTOGRAM(key)) AS key_histogram,
    Histogram::Print(HISTOGRAM(subkey)) AS subkey_histogram,
    Histogram::Print(HISTOGRAM(value), 50) AS value_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
