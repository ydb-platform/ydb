/* ytfile can not */
/* dqfile can not */
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT key, subkey, some(value) as value
FROM Input
GROUP BY key, subkey
ORDER BY key, subkey;
