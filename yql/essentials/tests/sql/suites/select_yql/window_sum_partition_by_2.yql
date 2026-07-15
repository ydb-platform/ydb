PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b) OVER (
        PARTITION BY
            c,
            d
    )
FROM (
    VALUES
        (1, 001, 1, 1),
        (2, 002, 2, 1),
        (2, 020, 2, 2),
        (3, 003, 3, 1),
        (3, 030, 3, 2),
        (3, 300, 3, 2)
) AS x (
    a,
    b,
    c,
    d
);
