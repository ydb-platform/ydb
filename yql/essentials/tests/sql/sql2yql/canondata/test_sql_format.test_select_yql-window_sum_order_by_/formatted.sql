PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b) OVER (
        ORDER BY
            c
    )
FROM (
    VALUES
        (1, 001, 3),
        (2, 002, 2),
        (2, 020, 2),
        (3, 003, 1),
        (3, 030, 1),
        (3, 300, 1)
) AS x (
    a,
    b,
    c
);
