$input = [
    <|value: "01238"u|>,
    <|value: "01239"u|>,
];

SELECT
    value as value,
    Unicode::ToUint64(value),
FROM AS_TABLE($input)
