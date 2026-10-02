PRAGMA YqlSelect = 'force';

SELECT
    (
        SELECT
            *
        FROM (
            SELECT
                1 AS a
        ) AS x
        WHERE
            x.a + 1 == y.b
    )
FROM (
    VALUES
        (2)
) AS y (
    b
);
