/* postgres can not */
USE plato;

SELECT key, some(value) as s FROM Input GROUP BY key ORDER BY key, s;

SELECT key, sum(cast(subkey as Int32)) as s FROM Input WHERE key > "100" GROUP BY key ORDER BY key, s;

SELECT key, some(subkey) as s FROM Input WHERE key > "100" GROUP BY key ORDER BY key, s;
