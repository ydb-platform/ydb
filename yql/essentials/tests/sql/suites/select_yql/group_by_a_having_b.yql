PRAGMA YqlSelect = 'force';

SELECT
    a,
    Count(b)
FROM (
    VALUES
        (1, 001),
        (1, 010),
        (1, 100),
        (2, 002),
        (2, 020),
        (2, 200),
        (3, 003),
        (3, 030),
        (3, 300)
) AS x (
    a,
    b
)
GROUP BY
    a
HAVING
    Sum(b) BETWEEN 100 AND 300
;
