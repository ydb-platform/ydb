PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    ib.*
FROM
    Input AS ia
JOIN
    Input AS ib
ON
    Unicode::ToUpper(CAST(ia.key AS Utf8)) == ib.subkey
ORDER BY
    ib.key,
    ib.subkey
;
