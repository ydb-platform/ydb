PRAGMA YqlSelect = 'force';

-- (1,1), (2,3), (3,6)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS UNBOUNDED PRECEDING
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- Unsupported frame type: range
-- (1,1), (2,5), (2,5), (3,8)
-- SELECT a, SUM(a) OVER (ORDER BY a RANGE UNBOUNDED PRECEDING) AS result
-- FROM (VALUES (1),(2),(2),(3)) AS x(a);
-- (1,1), (2,3), (3,5)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS 1 PRECEDING
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- (1,6), (2,6), (3,6)
SELECT
    a,
    Sum(a) OVER (
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- (1,1), (2,3), (3,6)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- (1,3), (2,6), (3,5)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- FIXME(YQL-21046): List<Struct<'a':Int32,'result':Int64>>, but it is: List<Struct<'a':Int32,'result':Int64?>>
-- (1,NULL), (2,1), (3,3)
-- SELECT a, Sum(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS result
-- FROM (VALUES (1),(2),(3)) AS x(a);
-- (1,6), (2,5), (3,3)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- (1,1), (2,2), (3,3)
SELECT
    a,
    Sum(a) OVER (
        ROWS BETWEEN CURRENT ROW AND CURRENT ROW
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);

-- Unsupported frame type: range
-- (1,3), (2,3), (4,4)
-- SELECT a, SUM(a) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS result
-- FROM (VALUES (1),(2),(4)) AS x(a);
-- (1,1), (2,3), (3,6)
SELECT
    a,
    Sum(a) OVER (
        ORDER BY
            a
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS
    ) AS result
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
);
