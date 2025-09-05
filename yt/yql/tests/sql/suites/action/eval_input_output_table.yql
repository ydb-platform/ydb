/* syntax version 1 */
/* postgres can not */
USE plato;

$a = CAST(Unicode::ToUpper("o"u) AS String) || "utput";
$b = CAST(Unicode::ToUpper("i"u) AS String) || "nput";
INSERT INTO $a
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM $b
WHERE key < "100"
ORDER BY key;
