/* postgres can not */
SELECT
    *
FROM plato.Input
WHERE key IN ParseFile('String', "keyid.lst");
