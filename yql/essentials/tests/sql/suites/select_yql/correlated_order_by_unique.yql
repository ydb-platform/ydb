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
ORDER BY
    (
        SELECT
            w
        FROM (
            VALUES
                (1, 20),
                (3, 10),
                (2, 30)
        ) AS weight (
            a,
            w
        )
        WHERE
            x.a == weight.a
    )
;
