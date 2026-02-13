PRAGMA YqlSelect = 'force';

SELECT
    a,
    b IN (
        SELECT
            y.b / 10,
        FROM (
            VALUES
                (1, 100),
                (3, 300),
                (5, 500)
        ) AS y (
            a,
            b
        )
    )
FROM (
    VALUES
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40),
        (5, 50)
) AS x (
    a,
    b
);
