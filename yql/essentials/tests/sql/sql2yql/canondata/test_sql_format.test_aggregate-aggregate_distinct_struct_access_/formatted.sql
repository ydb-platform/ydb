/* syntax version 1 */
/* postgres can not */
USE plato;

$withStruct =
    SELECT
        subkey,
        value,
        AsStruct(key AS key) AS s
    FROM Input3;

SELECT
    count(DISTINCT s.key) AS cnt
FROM $withStruct;
