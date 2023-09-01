/* syntax version 1 */
SELECT
    HISTOGRAM(key, 1) AS key_histogram,
    HISTOGRAM(subkey, 3) AS subkey_histogram,
    HISTOGRAM(value, 1000000) AS value_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
