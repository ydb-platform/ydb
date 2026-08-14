$input = [
    <|value: "test"u|>,
    <|value: "\xD1\x82\xD0\xB5\xD1\x81\xD1\x82"u|>,
    <|value: "TeSt"u|>,
    <|value: "\xD1\x82\xD0\x95\xD1\x81\xD0\xA2"u|>,
    <|value: "Eyl\xC3\xBCl"u|>,
    <|value: "6"u|>,
    <|value: ""u|>,
];

SELECT
    value,
    Unicode::ToLower(value) AS lower,
    Unicode::ToUpper(value) AS upper,
    Unicode::ToTitle(value) AS title,
    Unicode::Reverse(value) AS reverse,
FROM AS_TABLE($input);
