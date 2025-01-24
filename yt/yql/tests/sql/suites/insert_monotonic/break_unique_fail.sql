/* ytfile can not */
/* custom error:Duplicate key*/
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT DISTINCT * FROM Input
ORDER BY key, subkey;
