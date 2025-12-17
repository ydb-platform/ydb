/* custom error: type should be : List<Struct<'a.a':Int32?,'b.b':Int32?,'c.c':Int32?>>, but it is: List<Struct<'a.a':Int32?,'b.b':Int32?,'c.c':Int32>> */
-- YQL has no IMPLICIT CROSS JOIN (COMMA) over JOIN precedence.
PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test. The problem with RIGHT JOIN.
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
