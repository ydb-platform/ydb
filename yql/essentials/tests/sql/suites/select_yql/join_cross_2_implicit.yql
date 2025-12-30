PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

SELECT
    a,
    b
FROM (
    VALUES
        (01),
        (02)
) AS x (
    a
)
, (
    VALUES
        (10),
        (20)
) AS y (
    b
);
