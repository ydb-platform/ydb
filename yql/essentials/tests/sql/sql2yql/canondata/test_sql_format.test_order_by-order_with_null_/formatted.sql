/* postgres can not */
USE plato;

$input = (
    SELECT
        NULL AS key,
        "0" AS subkey,
        "kkk" AS value
    UNION ALL
    SELECT
        *
    FROM Input
);

SELECT
    *
FROM $input
ORDER BY
    key ASC;

SELECT
    *
FROM $input
ORDER BY
    key DESC;
