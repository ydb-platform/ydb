/* postgres can not */
USE plato;

$i = (
    SELECT
        *
    FROM Input
    WHERE key == "0"
    ORDER BY
        key
    LIMIT 100
);

SELECT
    key,
    some(value)
FROM $i
GROUP BY
    key;

SELECT
    key,
    some(subkey)
FROM $i
GROUP BY
    key;
