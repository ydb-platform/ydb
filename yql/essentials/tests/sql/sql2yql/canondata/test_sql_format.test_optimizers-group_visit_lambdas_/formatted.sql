/* postgres can not */
USE plato;

SELECT
    key,
    subkey,
    value,
    TablePath() AS path
FROM
    range("", "Input1", "Input5")
WHERE
    key != ""
ORDER BY
    key,
    subkey,
    path
;
