/* syntax version 1 */
/* postgres can not */
USE plato;

$data = (SELECT * FROM Input1 LIMIT 10);

SELECT key, subkey, _other["value"] FROM $data LIMIT 100;

SELECT a.key AS key, b.subkey AS subkey, b.value AS value
FROM $data AS a
INNER JOIN Input2 AS b ON a.key = b.key
ORDER BY key;
