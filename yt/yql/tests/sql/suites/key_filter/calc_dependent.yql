/* postgres can not */
USE plato;

$k1 = "3" || "23";
$k2 = "0" || SUBSTRING($k1, 1);

SELECT
    key
FROM Input
WHERE key >= $k2 and key <= $k1;
