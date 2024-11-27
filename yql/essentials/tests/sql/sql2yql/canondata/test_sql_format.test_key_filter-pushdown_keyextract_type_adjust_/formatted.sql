/* syntax version 1 */
/* postgres can not */
USE plato;

$src =
    SELECT
        Just(key) AS key,
        "ZZZ" || subkey AS subkey,
        value
    FROM Input
        AS u
    ASSUME ORDER BY
        key,
        subkey,
        value;

SELECT
    *
FROM $src
WHERE key > "023" AND key < "150"
ORDER BY
    key;
