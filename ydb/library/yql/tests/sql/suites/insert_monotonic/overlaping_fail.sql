/* ytfile can not */
/* dqfile can not */
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT * FROM Input
ORDER BY key, subkey;