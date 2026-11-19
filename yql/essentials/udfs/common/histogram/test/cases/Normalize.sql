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
    Histogram::Normalize(HISTOGRAM(key)) AS key_histogram,
    Histogram::Normalize(HISTOGRAM(subkey), 1.0) AS subkey_histogram,
    Histogram::Normalize(HISTOGRAM(value), -1.0) AS value_histogram
FROM (
    SELECT
        CAST(key AS Double) AS key,
        CAST(subkey AS Double) AS subkey,
        CAST(value AS Double) AS value
    FROM AS_TABLE($input)
);
