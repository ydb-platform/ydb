/* syntax version 1 */
/* postgres can not */
USE plato;

$dictList = (
    SELECT
        AsDict(AsTuple(value, CAST(subkey AS Int32))) AS `dict`,
        subkey,
        value
    FROM
        Input
);

SELECT
    input.`dict`[input.value],
    input.`dict`[input.subkey]
FROM
    $dictList AS input
;
