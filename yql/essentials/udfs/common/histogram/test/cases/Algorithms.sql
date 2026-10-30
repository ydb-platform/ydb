$input = [
    <|key: "0", subkey: "0", value: "-1"|>,
    <|key: "1", subkey: "1", value: "1"|>,
    <|key: "2", subkey: "2", value: "2"|>,
    <|key: "3", subkey: "4", value: "8"|>,
    <|key: "4", subkey: "8", value: "32"|>,
    <|key: "5", subkey: "16", value: "128"|>,
    <|key: "6", subkey: "32", value: "512"|>,
    <|key: "7", subkey: "64", value: "2048"|>,
    <|key: "7", subkey: "64", value: "2048"|>,
    <|key: "7", subkey: "64", value: "2048"|>,
    <|key: "7", subkey: "64", value: "2048"|>,
    <|key: "7", subkey: "64", value: "2048"|>,
    <|key: "2", subkey: "2", value: "2048"|>,
    <|key: "3", subkey: "4", value: "2048"|>,
    <|key: "2", subkey: "2", value: "2048"|>,
    <|key: "3", subkey: "4", value: "2048"|>,
];

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
    FROM AS_TABLE($input)
);
