PRAGMA YqlSelect = 'force';

SELECT
    Count(b)
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    b
)
HAVING
    Sum(b) < 0
;
