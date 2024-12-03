/* syntax version 1 */
PRAGMA DisableSimpleColumns;
USE plato;

SELECT
    *
FROM Input1
    AS a
RIGHT JOIN Input2
    AS b
ON a.k1 == b.k2
ORDER BY
    a.v1,
    b.v2;

SELECT
    *
FROM ANY Input1
    AS a
RIGHT JOIN Input2
    AS b
ON a.k1 == b.k2
ORDER BY
    a.v1,
    b.v2;

SELECT
    *
FROM Input1
    AS a
RIGHT JOIN ANY Input2
    AS b
ON a.k1 == b.k2
ORDER BY
    a.v1,
    b.v2;

SELECT
    *
FROM ANY Input1
    AS a
RIGHT JOIN ANY Input2
    AS b
ON a.k1 == b.k2
ORDER BY
    a.v1,
    b.v2;
