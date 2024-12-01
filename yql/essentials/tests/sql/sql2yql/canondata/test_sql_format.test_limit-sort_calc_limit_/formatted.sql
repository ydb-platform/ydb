/* postgres can not */
USE plato;

INSERT INTO Output WITH truncate
SELECT
    *
FROM Input
ORDER BY
    key || subkey
LIMIT 2;
