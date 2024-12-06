PRAGMA DisableSimpleColumns;
USE plato;

FROM (
    SELECT
        key AS akey1,
        key AS akey2,
        subkey || key AS subkey,
        value
    FROM
        Input1
) AS a
LEFT JOIN (
    SELECT
        key || subkey AS subkey,
        key AS bkey1,
        key AS bkey2,
        1 AS value
    FROM
        Input2
) AS b
ON
    a.akey1 == b.bkey1 AND a.akey1 == b.bkey2 AND a.akey2 == b.bkey1
SELECT
    a.akey1 AS akey1,
    b.bkey1 AS bkey1,
    a.subkey,
    b.subkey,
    b.value
ORDER BY
    akey1,
    bkey1
;
