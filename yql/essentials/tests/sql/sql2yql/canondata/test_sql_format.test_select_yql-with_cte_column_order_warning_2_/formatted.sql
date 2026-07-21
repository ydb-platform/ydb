PRAGMA YqlSelect = 'force';

WITH x (a, b) AS (
    SELECT
        1,
        '2'
)
SELECT
    1
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1,
        '2' AS b
)
SELECT
    1
FROM
    x
;

WITH x (a, b) AS (
    SELECT
        1,
        '2' AS c
)
SELECT
    1
FROM
    x
;
