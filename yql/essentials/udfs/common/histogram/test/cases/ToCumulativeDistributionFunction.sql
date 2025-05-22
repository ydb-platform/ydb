/* syntax version 1 */
SELECT
    Histogram::ToCumulativeDistributionFunction(HISTOGRAM(key)) AS key_histogram,
    Histogram::ToCumulativeDistributionFunction(HISTOGRAM(subkey)) AS subkey_histogram,
    Histogram::ToCumulativeDistributionFunction(HISTOGRAM(value)) AS value_histogram,
    Histogram::Normalize(Histogram::ToCumulativeDistributionFunction(HISTOGRAM(subkey)), 100, True) AS subkey_norm_cdf_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
