PRAGMA DisableSimpleColumns;

USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value
    FROM
        Input1
) AS a
JOIN (
    SELECT
        key || subkey AS subkey,
        key,
        1 AS value
    FROM
        Input2
) AS b
ON
    a.key == b.key
SELECT
    a.key AS akey,
    b.key AS bkey,
    a.subkey,
    b.subkey,
    b.value
ORDER BY
    akey
;
