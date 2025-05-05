USE plato;

$i = SELECT
    a.key as key,
    b.subkey as subkey
FROM Input1 AS a
JOIN Input2 AS b ON a.key = b.key
ASSUME ORDER BY key, subkey;

$i = SELECT * FROM $i WHERE key != subkey ASSUME ORDER BY key, subkey;

SELECT DISTINCT key, subkey FROM $i;
SELECT COUNT(*) FROM $i;
