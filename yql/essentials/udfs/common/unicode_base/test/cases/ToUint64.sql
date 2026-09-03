$input = [
    <|key: "with_format_1"u, value: "0x1234abcd"u|>,
    <|key: "with_format_2"u, value: "0X4"u|>,
    <|key: "with_format_3"u, value: "0644"u|>,
    <|key: "binary_1"u, value: "0101010"u|>,
    <|key: "binary_2"u, value: "101"u|>,
    <|key: "zero"u, value: "0"u|>,
];

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value)
FROM AS_TABLE($input)
WHERE key = "with_format_1" 
   OR key = "with_format_2"
   OR key = "with_format_3"
   OR key = "binary_1"
   OR key = "binary_2";

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value, 2),
    Unicode::ToUint64(value, 16)
FROM AS_TABLE($input)
WHERE key = "binary_1" 
   OR key = "binary_2";

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value, 8),
    Unicode::ToUint64(value, 10),
    Unicode::ToUint64(value, 16)
FROM AS_TABLE($input)
WHERE key = "zero";
