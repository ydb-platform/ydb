/* custom error:Insert with "monotonic_keys" setting cannot be used with a non-existent table*/
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT 1 as key
ORDER BY key;
