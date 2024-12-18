/* syntax version 1 */
/* postgres can not */
USE plato;

$dictList = (
    SELECT
        AsDict(AsTuple(value, CAST(subkey AS Int32))) AS `dict`,
        AsDict(AsTuple('z', 'a'), AsTuple('y', 'b')) AS d,
        subkey,
        value
    FROM
        Input
);

SELECT
    d['z'] AS static,
    input.`dict`[input.value] AS dynamic,
    input.`dict` AS `dict`
FROM
    $dictList AS input
;
