/* custom error: List items types isn't same: Tuple<Int32,Int32> and Tuple<Int32,Null> */
PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test, postgres can.
SELECT
    a,
    Count(b)
FROM (
    VALUES
        (1, 11),
        (2, NULL),
        (2, 22),
        (3, NULL),
        (3, NULL),
        (3, 33)
) AS x (
    a,
    b
)
GROUP BY
    a
;
