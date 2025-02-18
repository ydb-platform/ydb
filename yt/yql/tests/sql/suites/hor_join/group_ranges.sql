/* postgres can not */
/* kikimr can not */
USE plato;

$i = (SELECT * FROM plato.range(``, Input1, Input4));

SELECT * FROM (
    SELECT 1 as key, subkey, value FROM $i
    UNION ALL
    SELECT 2 as key, subkey, value FROM $i
) AS x
ORDER BY key, subkey, value
;