PRAGMA DisableSimpleColumns;
USE plato;

FROM
    Input1 AS a
LEFT JOIN (
    SELECT
        key,
        NULL AS subkey,
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
