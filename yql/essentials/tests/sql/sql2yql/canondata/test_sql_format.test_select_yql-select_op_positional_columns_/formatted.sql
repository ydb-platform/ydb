PRAGMA YqlSelect = 'force';
PRAGMA config.flags('PositionalUnionAll');

SELECT
    1 AS a,
    2 AS b
UNION ALL
SELECT
    3 AS b,
    4 AS a
ORDER BY
    a
;

SELECT
    1 AS a
UNION ALL
SELECT
    2 AS b
;

SELECT
    1 AS a
EXCEPT ALL
SELECT
    1 AS b
;

SELECT
    1 AS a
INTERSECT ALL
SELECT
    1 AS b
;
