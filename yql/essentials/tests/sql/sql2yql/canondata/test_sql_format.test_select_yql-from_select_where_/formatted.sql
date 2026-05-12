PRAGMA YqlSelect = 'force';

SELECT
    a,
FROM (
    SELECT
        3 AS a,
        4 AS b
)
WHERE
    2 <= a
    AND a <= 4
;
