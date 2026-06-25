PRAGMA YqlSelect = 'force';

WITH x (a) AS (
    SELECT
        1
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1,
        '2'
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1 AS a,
        '2' AS b
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1 AS b,
        '2' AS a
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1 AS c,
        '2' AS d
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1 AS a,
        '2' AS b
    UNION ALL
    SELECT
        11 AS a,
        '22' AS b
)
SELECT
    *
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1,
        '2'
    UNION ALL
    SELECT
        11,
        '22'
)
SELECT
    *
FROM
    x
;
