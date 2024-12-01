/* custom error:Insert with "monotonic_keys" setting cannot be used with a non-existent table*/
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT
    1 AS key
ORDER BY
    key;
