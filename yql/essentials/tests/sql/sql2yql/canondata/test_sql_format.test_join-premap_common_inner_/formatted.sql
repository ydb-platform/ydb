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
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    a.value,
    b.value
ORDER BY
    a.key
;
