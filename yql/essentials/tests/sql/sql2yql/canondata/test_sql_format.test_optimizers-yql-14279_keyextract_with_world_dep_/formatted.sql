/* postgres can not */
USE plato;

$input =
    SELECT
        *
    FROM
        range("", "Input1", "Input2")
;

$key =
    SELECT
        min(key)
    FROM
        $input
;

SELECT
    key,
    subkey,
    value
FROM
    $input
WHERE
    subkey > '1' AND key > $key
ORDER BY
    key
;
