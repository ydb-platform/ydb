$input = [
    <|value: "0x1234abcd"u|>,
    <|value: "0X4"u|>,
    <|value: "0644"u|>,
    <|value: "0101010"u|>,
    <|value: "101"u|>,
    <|value: "0"u|>,
    <|value: "hell"u|>,
    <|value: "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"u|>,
];

SELECT
    value as value,
    Unicode::TryToUint64(value, 10),
    Unicode::TryToUint64(value, 1),
    Unicode::TryToUint64(value, 4),
    Unicode::TryToUint64(value, 8),
    Unicode::TryToUint64(value, 16)
FROM AS_TABLE($input)
