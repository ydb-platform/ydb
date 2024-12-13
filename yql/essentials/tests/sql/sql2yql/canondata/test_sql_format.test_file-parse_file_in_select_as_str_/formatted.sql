/* postgres can not */
SELECT
    key,
    value,
    key IN ParseFile('string', 'keyid.lst') AS privilege
FROM
    plato.Input
;
