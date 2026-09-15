PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b) OVER (
        PARTITION BY
            c
        ORDER BY
            d
    )
FROM (
    VALUES
        (11, 001, 1, 31),
        (21, 002, 2, 21),
        (22, 020, 2, 22),
        (31, 003, 3, 11),
        (32, 030, 3, 12),
        (33, 300, 3, 13)
) AS x (
    a,
    b,
    c,
    d
);
