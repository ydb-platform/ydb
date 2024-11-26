/* postgres can not */
/* ignore runonopt ast diff */
/* ignore runonopt plan diff */
USE plato;

$i = (
    SELECT
        *
    FROM Input
    WHERE key > "100"
);

SELECT
    *
FROM $i
ORDER BY
    key
LIMIT 1;

SELECT
    *
FROM $i
ORDER BY
    subkey
LIMIT 2;
