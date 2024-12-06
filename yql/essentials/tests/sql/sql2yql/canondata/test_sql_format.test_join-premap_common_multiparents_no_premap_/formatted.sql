PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;

$map = (
    SELECT
        key,
        subkey,
        1 AS value,
        2 AS another
    FROM Input1
);

FROM $map
    AS a
JOIN Input2
    AS b
USING (key)
SELECT
    a.key,
    a.value,
    b.value
ORDER BY
    a.key,
    a.value;

FROM $map
    AS a
SELECT
    a.key,
    a.value,
    a.subkey
ORDER BY
    a.key,
    a.value;
