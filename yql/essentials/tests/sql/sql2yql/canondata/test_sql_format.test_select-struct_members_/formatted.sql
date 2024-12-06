/* postgres can not */
USE plato;

$structList = (
    SELECT
        AsStruct(key AS k, value AS v) AS `struct`
    FROM Input
);

SELECT
    input.`struct`.k AS key,
    input.`struct`.v AS value,
    input.`struct` AS `struct`
FROM $structList
    AS input;
