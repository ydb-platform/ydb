/* syntax version 1 */
USE plato;

SELECT
    histogram_cdf(CAST(key AS double)) AS key,
    adaptive_ward_histogram_cdf(CAST(subkey AS double)) AS subkey
FROM Input4;
