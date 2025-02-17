/* syntax version 1 */
SELECT
    HISTOGRAM(key) AS key_histogram,
    HISTOGRAM(subkey) AS subkey_histogram,
    HISTOGRAM(value) AS value_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
