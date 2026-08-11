$input = [
    <|value: "строка без внешних пробелов"u|>,
    <|value: " только левый пробел"u|>,
    <|value: "только правый пробел "u|>,
    <|value: "строка_совсем_без_пробелов"u|>,
    <|value: " юникод+перевод строки\n"u|>,
    <|value: ""u|>,
];

SELECT
    value as value,
    Unicode::Strip(value)
FROM AS_TABLE($input)
