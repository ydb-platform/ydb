PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    ib.*
FROM Input as ia
JOIN Input as ib
ON Unicode::ToUpper(CAST(ia.key AS Utf8)) == ib.subkey
ORDER BY ib.key, ib.subkey
