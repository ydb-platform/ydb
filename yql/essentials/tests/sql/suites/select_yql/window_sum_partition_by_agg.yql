PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(a) OVER (
        PARTITION BY
            Sum(b)
    ) AS window_group_sum,
    Sum(b) AS group_sum
FROM (
    VALUES
        (0 + 1, 001),
        (0 + 2, 002),
        (0 + 2, 020),
        (0 + 3, 003),
        (0 + 3, 030),
        (0 + 3, 300),
        (3 + 1, 001),
        (3 + 2, 002),
        (3 + 2, 020),
        (3 + 3, 003),
        (3 + 3, 030),
        (3 + 3, 300)
) AS x (
    a,
    b
)
GROUP BY
    a
;
