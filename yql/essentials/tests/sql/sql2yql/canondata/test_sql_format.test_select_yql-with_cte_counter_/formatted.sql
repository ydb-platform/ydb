PRAGMA YqlSelect = 'force';

WITH RECURSIVE counter AS (
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1 AS n
    FROM
        counter
    WHERE
        n < 5
)
SELECT
    *
FROM
    counter
;
