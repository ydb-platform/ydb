PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

$keys = (
    SELECT
        key
    FROM
        Input3
);

FROM (
    SELECT
        key,
        value,
        key IN $keys AS flag
    FROM
        Input1
) AS a
RIGHT JOIN (
    SELECT
        key,
        key IN $keys AS flag
    FROM
        Input2
) AS b
USING (key)
SELECT
    a.key,
    a.value,
    a.flag,
    b.flag
ORDER BY
    a.key
;
