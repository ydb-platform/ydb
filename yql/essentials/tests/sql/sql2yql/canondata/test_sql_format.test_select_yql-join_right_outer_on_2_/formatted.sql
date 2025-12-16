/* custom error: type should be : List<Struct<'x.a':Int32?,'y.b':Int32?>>, but it is: List<Struct<'x.a':Int32?,'y.b':Int32>> */
PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test. The problem with RIGHT JOIN.
SELECT
    x.a,
    y.b,
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
)
RIGHT OUTER JOIN (
    VALUES
        (1),
        (2),
        (4)
) AS y (
    b
)
ON
    x.a == y.b
;
