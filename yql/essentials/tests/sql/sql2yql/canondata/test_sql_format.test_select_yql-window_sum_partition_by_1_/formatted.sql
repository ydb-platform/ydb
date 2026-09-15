PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b) OVER (
        PARTITION BY
            c
    )
FROM (
    VALUES
        (1, 001, 1),
        (2, 002, 2),
        (2, 020, 2),
        (3, 003, 3),
        (3, 030, 3),
        (3, 300, 3)
) AS x (
    a,
    b,
    c
);
