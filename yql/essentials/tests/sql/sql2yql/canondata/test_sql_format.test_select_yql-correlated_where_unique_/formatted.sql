PRAGMA YqlSelect = 'force';

SELECT
    a
FROM (
    VALUES
        (1, 10),
        (2, 20),
        (3, 30)
) AS x (
    a,
    b
)
WHERE
    b == (
        SELECT
            y.b / 10,
        FROM (
            VALUES
                (1, 100),
                (2, 200),
                (3, 300)
        ) AS y (
            a,
            b
        )
        WHERE
            x.a == y.a
    )
;
