PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;

FROM (
    SELECT
        key AS akey,
        subkey,
        value AS avalue
    FROM Input1
)
    AS a
CROSS JOIN Input2
    AS b
SELECT
    a.akey,
    a.subkey,
    b.subkey,
    b.value
ORDER BY
    a.akey,
    a.subkey,
    b.subkey,
    b.value;
