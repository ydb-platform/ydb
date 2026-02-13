PRAGMA YqlSelect = 'force';

SELECT
    (
        SELECT
            x.b * 1000 + y.b,
        FROM (
            VALUES
                (1, 11),
                (2, 22),
                (3, 33)
        ) AS y (
            a,
            b
        )
        WHERE
            x.a == y.a
    )
FROM (
    VALUES
        (1, 1),
        (2, 2),
        (3, 3)
) AS x (
    a,
    b
);
