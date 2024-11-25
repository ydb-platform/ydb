/* ytfile can not */
/* custom error:job outputs overlap with original table*/
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT * FROM Input
ORDER BY key, subkey;
