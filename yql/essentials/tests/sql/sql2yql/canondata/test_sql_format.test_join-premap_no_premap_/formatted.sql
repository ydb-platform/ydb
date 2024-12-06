PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

-- not renaming
FROM (
    SELECT
        key,
        subkey || key AS subkey,
        value
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    b.value
ORDER BY
    a.key
;

-- not fixed size
FROM (
    SELECT
        key,
        "aaa" AS subkey,
        value
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    b.value
ORDER BY
    a.key
;

-- to many Justs
FROM (
    SELECT
        key,
        Just(Just(1)) AS subkey,
        value
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    b.value
ORDER BY
    a.key
;

-- container
FROM (
    SELECT
        key,
        AsTuple(1) AS subkey,
        value
    FROM
        Input1
) AS a
JOIN
    Input2 AS b
USING (key)
SELECT
    a.key,
    a.subkey,
    b.value
ORDER BY
    a.key
;
