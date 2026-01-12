/* custom error: Error: Member not found: _yql_join_sublink_0__alias_x.b. */
PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test.
SELECT
    a,
    b IN (
        SELECT
            y.b / 10
        FROM (
            VALUES
                (1, 100),
                (1, 300),
                (1, 500),
                (2, 100),
                (2, 300),
                (2, 500),
                (3, 100),
                (3, 300),
                (3, 500)
        ) AS y (
            a,
            b
        )
        WHERE
            x.a == y.a
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
