/* ytfile can not */
/* dqfile can not */
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT DISTINCT * FROM Input
ORDER BY key, subkey;