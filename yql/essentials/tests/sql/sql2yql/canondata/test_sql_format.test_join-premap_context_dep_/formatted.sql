PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value,
        TableRecordIndex() AS tr
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    a.tr,
    b.value
ORDER BY
    a.key,
    a.tr
;
