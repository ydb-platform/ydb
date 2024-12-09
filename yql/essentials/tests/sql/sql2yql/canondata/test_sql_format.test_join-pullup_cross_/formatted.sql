PRAGMA DisableSimpleColumns;
USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value || "v" AS value
    FROM Input1
)
    AS a
CROSS JOIN Input2
    AS b
SELECT
    a.key,
    a.subkey,
    b.subkey,
    b.value
ORDER BY
    a.key,
    a.subkey,
    b.subkey,
    b.value;
