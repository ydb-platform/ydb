/* syntax version 1 */
$digits = Pire::Grep("\\d+");
$spaces = Pire::Grep("\\s+");

SELECT 
    value,
    $digits(value) AS digits,
    $spaces(value) AS spaces
FROM Input;
