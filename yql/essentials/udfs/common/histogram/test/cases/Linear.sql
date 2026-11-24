$input = [
    <|key: "0", subkey: "0", value: "-3"|>,
    <|key: "0", subkey: "0", value: "-49"|>,
    <|key: "2", subkey: "2", value: "2"|>,
    <|key: "3", subkey: "4", value: "8"|>,
    <|key: "4", subkey: "8", value: "32"|>,
    <|key: "5", subkey: "16", value: "88"|>,
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
    <|key: "0", subkey: "0", value: "-2049"|>,
    <|key: "0", subkey: "0", value: "-2149"|>,
];

SELECT
    LinearHistogram(value) AS default,
    LinearHistogram(value, 33) AS linear_size,
    LinearHistogram(value, 100, 100, 1000) AS linear_min_max
FROM (
    SELECT
        CAST(value AS Double) AS value
    FROM AS_TABLE($input)
);
