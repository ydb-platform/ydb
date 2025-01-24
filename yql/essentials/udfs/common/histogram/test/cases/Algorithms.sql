/* syntax version 1 */
SELECT
    ADAPTIVE_DISTANCE_HISTOGRAM(key, 3) AS adaptive_distance,
    ADAPTIVE_WEIGHT_HISTOGRAM(key, 3) AS adaptive_weight,
    ADAPTIVE_WARD_HISTOGRAM(key, 3) AS adaptive_ward,
    BLOCK_WEIGHT_HISTOGRAM(key, 3) AS block_weight,
    BLOCK_WARD_HISTOGRAM(key, 3) AS block_ward,
    Histogram::Print(ADAPTIVE_WEIGHT_HISTOGRAM(key, 3)) <> Histogram::Print(BLOCK_WEIGHT_HISTOGRAM(key, 3)) AS algo_equality_check,
    Histogram::Print(ADAPTIVE_WEIGHT_HISTOGRAM(key, 3)) <> Histogram::Print(ADAPTIVE_WARD_HISTOGRAM(key, 3)) AS quality_equality_check
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM Input
);
