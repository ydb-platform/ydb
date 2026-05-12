-- YQL has no IMPLICIT CROSS JOIN (COMMA) over JOIN precedence.
PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

SELECT
    a.a,
    b.b,
    c.c
FROM (
    VALUES
        (01)
) AS a (
    a
)
, (
    VALUES
        (10)
) AS b (
    b
)
RIGHT JOIN (
    VALUES
        (10),
        (00)
) AS c (
    c
)
ON
    b.b == c.c
;
