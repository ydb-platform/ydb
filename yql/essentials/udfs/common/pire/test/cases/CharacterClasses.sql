$input = [
    <|value: "xx000xx"|>,
    <|value: "lLlLl"|>,
    <|value: "a1 b2 c3"|>,
    <|value: "xxx yyy"|>,
];

$digits = Pire::Grep("\\d+");
$spaces = Pire::Grep("\\s+");

SELECT
    value,
    $digits(value) AS digits,
    $spaces(value) AS spaces
FROM AS_TABLE($input);
