SELECT
    r,
    x,
    nth_value(x, 1) OVER w AS nr1,
    nth_value(x, 1) IGNORE NULLS OVER w AS ni1,
    nth_value(x, 2) OVER w AS nr2,
    nth_value(x, 2) IGNORE NULLS OVER w AS ni2,
    nth_value(x, 3) OVER w AS nr3,
    nth_value(x, 3) IGNORE NULLS OVER w AS ni3,
    nth_value(x, 4) OVER w AS nr4,
    nth_value(x, 4) IGNORE NULLS OVER w AS ni4
FROM (
    VALUES
        (1, 3),
        (2, NULL),
        (3, 4),
        (4, 5)
) AS a (
    r,
    x
)
WINDOW
    w AS (
        ORDER BY
            r
    )
ORDER BY
    r
;
