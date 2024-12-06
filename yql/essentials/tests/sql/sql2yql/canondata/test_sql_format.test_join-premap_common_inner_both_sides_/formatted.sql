PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;

FROM (
    SELECT
        key,
        subkey,
        1 AS value
    FROM
        Input1
) AS a
JOIN (
    SELECT
        key,
        subkey,
        2 AS value
    FROM
        Input2
) AS b
USING (key)
SELECT
    a.key AS key,
    a.subkey,
    a.value,
    b.subkey,
    b.value
ORDER BY
    key
;
