$input = [
    <|value: "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"u|>,
    <|value: "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"u|>,
];

SELECT
    value as value,
    Unicode::ToUint64(value),
FROM AS_TABLE($input)
