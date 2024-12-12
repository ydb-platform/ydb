PRAGMA DisableSimpleColumns;

USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value,
        TablePath() AS tp
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    a.tp,
    b.value
ORDER BY
    a.key,
    a.tp
;
