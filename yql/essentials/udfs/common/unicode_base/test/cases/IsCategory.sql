$input = [
    <|value: "0F3A4E"u|>,
    <|value: "вВаВыа"u|>,
    <|value: "фыв"u|>,
    <|value: "1234"u|>,
    <|value: "вы2в-а"u|>,
    <|value: "выа1-!ыв"u|>,
];

SELECT
    value as value,
    Unicode::IsAscii(value),
    Unicode::IsSpace(value),
    Unicode::IsUpper(value),
    Unicode::IsLower(value),
    Unicode::IsDigit(value),
    Unicode::IsAlpha(value),
    Unicode::IsAlnum(value),
    Unicode::IsHex(value),
    Unicode::IsUnicodeSet(value, "[вао]"u)
FROM AS_TABLE($input)
