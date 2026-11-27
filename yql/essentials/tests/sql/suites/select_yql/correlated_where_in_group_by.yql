PRAGMA YqlSelect = 'force';

SELECT
    a,
    b
FROM (
    VALUES
        (1, 111),
        (1, 000),
        (2, 222),
        (2, 000),
        (3, 333),
        (3, 000)
) AS y (
    a,
    b
)
WHERE
    (a, b) IN (
        SELECT
            (a, Sum(b))
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
        WHERE
            x.a == y.a
        GROUP BY
            a
    )
;
