USE plato;

$foo = (SELECT key, subkey, value IN ("wat", "bar") AS value FROM Input);

SELECT
   R0.key, R0.subkey, R0.value,
   R1.key, R1.subkey, R1.value,
   R2.key,            R2.value
FROM $foo AS R0
LEFT JOIN $foo AS R1 ON
   R0.subkey = R1.key
LEFT JOIN $foo AS R2 ON
   R1.subkey = R2.key
ORDER BY R0.key;

