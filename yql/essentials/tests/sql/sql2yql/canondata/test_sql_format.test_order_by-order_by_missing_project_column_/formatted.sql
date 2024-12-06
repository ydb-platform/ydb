USE plato;

SELECT
    subkey
FROM Input
ORDER BY
    key,
    value;

SELECT
    subkey
FROM Input
ORDER BY
    "x" || key,
    value;

SELECT
    subkey
FROM Input
ORDER BY
    key || "x"
LIMIT 3;

SELECT
    subkey
FROM Input
    AS a
ORDER BY
    "x" || key,
    a.value
LIMIT 3;

SELECT
    subkey
FROM Input
    AS a
ORDER BY
    a.key,
    value
LIMIT 1;

SELECT
    subkey
FROM Input
    AS a
ORDER BY
    key,
    value
LIMIT 2;

SELECT
    subkey,
    key
FROM Input
ORDER BY
    key,
    value;
