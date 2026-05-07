-- YQL has no IMPLICIT CROSS JOIN (COMMA) over JOIN precedence.
PRAGMA YqlSelect = 'force';

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
CROSS JOIN (
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
