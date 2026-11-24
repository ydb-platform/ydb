$input = [
    <|key: "0", subkey: "1000", value: "-1"|>,
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
    HISTOGRAM(key, value / subkey) AS basic_weight,
    HISTOGRAM(key, value, 3) AS weight_and_bins,
    Histogram::Print(HISTOGRAM(key)) <> Histogram::Print(HISTOGRAM(key, value)) AS equality_check
FROM (
    SELECT
        CAST(key AS Double) AS key,
        COALESCE(CAST(subkey AS Double), 1.0) AS subkey,
        COALESCE(CAST(value AS Double), 1.0) AS value
    FROM AS_TABLE($input)
);
