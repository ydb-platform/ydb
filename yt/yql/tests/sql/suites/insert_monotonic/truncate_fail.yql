/* custom error:"monotonic_keys" setting can not be used with TRUNCATE mode*/
USE plato;

INSERT INTO Output WITH (MONOTONIC_KEYS, TRUNCATE)
SELECT * FROM Input
ORDER BY key, subkey;
