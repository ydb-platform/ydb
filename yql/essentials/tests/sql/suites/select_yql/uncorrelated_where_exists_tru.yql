PRAGMA YqlSelect = 'force';

SELECT
    a,
    b
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
)
WHERE
    EXISTS (
        SELECT
            *
        FROM (
            VALUES
                (1)
        ) AS y (
            a
        )
        WHERE
            TRUE
    )
;
