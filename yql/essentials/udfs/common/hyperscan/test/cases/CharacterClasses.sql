/* syntax version 1 */
$digits = Hyperscan::Grep("\\d+");
$spaces = Hyperscan::Grep("\\s+");

SELECT
    value,
    $digits(value) AS digits,
    $spaces(value) AS spaces
FROM Input;
