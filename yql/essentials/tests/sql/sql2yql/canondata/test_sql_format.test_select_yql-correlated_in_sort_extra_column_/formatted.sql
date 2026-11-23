PRAGMA YqlSelect = 'force';

SELECT
    t.a
FROM (
    VALUES
        (1, 1),
        (2, 1)
) AS t (
    a,
    b
)
WHERE
    t.b IN (
        SELECT
            s.b
        FROM (
            VALUES
                (1, 1)
        ) AS s (
            a,
            b
        )
        WHERE
            s.a < t.a
        ORDER BY
            s.a DESC
    )
ORDER BY
    t.a
;
