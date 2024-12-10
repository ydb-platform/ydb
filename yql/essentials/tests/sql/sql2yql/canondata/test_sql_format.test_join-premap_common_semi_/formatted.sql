PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

FROM (
    SELECT
        key,
        subkey AS asubkey,
        value
    FROM
        Input1
) AS a
LEFT SEMI JOIN (
    SELECT
        key,
        1 AS value
    FROM
        Input2
) AS b
USING (key)
SELECT
    a.key,
    a.asubkey
ORDER BY
    a.key
;
