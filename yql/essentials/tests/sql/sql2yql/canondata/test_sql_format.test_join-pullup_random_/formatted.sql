/* kikimr can not - due to random */
PRAGMA DisableSimpleColumns;

USE plato;

FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value,
        RANDOM(value || "x") <= 1.0 AS rn
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    a.rn,
    b.value
ORDER BY
    a.key,
    a.rn
;
