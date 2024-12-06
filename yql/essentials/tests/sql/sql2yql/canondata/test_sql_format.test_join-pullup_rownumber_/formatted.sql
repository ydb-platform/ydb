PRAGMA DisableSimpleColumns;
USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value,
        ROW_NUMBER() OVER w AS rn
    FROM Input1
    WINDOW
        w AS ()
)
    AS a
JOIN Input2
    AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    a.rn,
    b.value
ORDER BY
    a.key,
    a.rn;
