/* syntax version 1 */
/* postgres can not */
USE plato;

$a = (SELECT CAST(Unicode::ToUpper("o"u) AS String) || "utpu");
$b = (SELECT CAST(Unicode::ToUpper("i"u) AS String) || "npu");
$a = $a || CAST(Unicode::ToLower("T"u) AS String);
$b = $b || CAST(Unicode::ToLower("T"u) AS String);
INSERT INTO $a
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM $b
WHERE key < "100"
ORDER BY key;
