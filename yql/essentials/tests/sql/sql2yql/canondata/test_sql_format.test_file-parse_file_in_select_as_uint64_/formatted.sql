/* postgres can not */
SELECT
    key,
    value,
    CAST(key AS int32) IN ParseFile('uint64', "keyid.lst") AS privilege
FROM
    plato.Input
;
