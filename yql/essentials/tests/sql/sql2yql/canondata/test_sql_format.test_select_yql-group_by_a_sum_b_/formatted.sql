PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b)
FROM (
    VALUES
        (1, 001),
        (2, 002),
        (2, 020),
        (3, 003),
        (3, 030),
        (3, 300)
) AS x (
    a,
    b
)
GROUP BY
    a
;
