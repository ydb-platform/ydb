/* postgres can not */
SELECT
    key,
    value,
    CAST(key AS int32) ?? 0 IN ParseFile('int32', "keyid.lst") AS privilege
FROM
    plato.Input
;
