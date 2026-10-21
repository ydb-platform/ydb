$input = [
    <|value: "hello"u|>,
    <|value: "meow"u|>,
];

SELECT
    value as value,
    Unicode::ToUint64(value),
FROM AS_TABLE($input)
