$input = [
    <|value: "0"u|>,
    <|value: "1"u|>,
];

SELECT
    value as value,
    Unicode::ToUint64(value, 1),
FROM AS_TABLE($input)
