PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

SELECT
    b,
    c
FROM (
    SELECT
        x.a AS b,
        y.a AS c
    FROM (
        VALUES
            (1)
    ) AS x (
        a
    )
    , (
        VALUES
            (2)
    ) AS y (
        a
    )
) AS z;
