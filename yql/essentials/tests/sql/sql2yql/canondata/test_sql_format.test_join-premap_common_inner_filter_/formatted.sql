PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value
    FROM Input1
    WHERE value != "ddd"
)
    AS a
JOIN Input2
    AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    b.value
ORDER BY
    a.key;
