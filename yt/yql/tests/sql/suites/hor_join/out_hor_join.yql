/* postgres can not */
USE plato;

$i = (SELECT * FROM Input WHERE key < "900");

SELECT key, some(value) as s FROM $i GROUP BY key ORDER BY key, s;

SELECT key, sum(cast(subkey as Int32)) as s FROM $i GROUP BY key ORDER BY key, s;

SELECT key, some(subkey) as s FROM $i GROUP BY key ORDER BY key, s;
