/* postgres can not */
SELECT
    *
FROM plato.Input
WHERE CAST(key AS Uint32) IN ParseFile('uint32', "keyid.lst");
