/* syntax version 1 */
/* postgres can not */
USE plato;

$a = CAST(Unicode::ToUpper("o"u) AS String) || "utput";
$b = CAST(Unicode::ToUpper("i"u) AS String) || "nput";

INSERT INTO $a
SELECT
    key AS key,
    "" AS subkey,
    "value:" || value AS value
FROM
    $b
WHERE
    key < "100"
ORDER BY
    key
;
