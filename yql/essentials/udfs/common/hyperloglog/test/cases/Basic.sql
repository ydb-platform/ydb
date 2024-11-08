/* syntax version 1 */
SELECT
    HyperLogLog(key) AS str,
    CountDistinctEstimate(CAST(subkey AS Double)) AS `double`,
    HLL(CAST(value AS Int64), 18) AS `int`
FROM Input;

