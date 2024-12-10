/* postgres can not */
USE plato;

SELECT
    row,
    -i64,
    +i64,
    abs(i64)
FROM
    BigDates
ORDER BY
    row
;

SELECT
    min(d32),
    min(dt64),
    min(ts64),
    min(i64),
    max(d32),
    max(dt64),
    max(ts64),
    max(i64)
FROM
    BigDates
;

SELECT
    l.row,
    r.row,
    1,
    l.d32 - r.i64,
    l.dt64 - r.i64,
    l.ts64 - r.i64,
    l.i64 - r.i64,
    2,
    l.d32 + r.i64,
    l.dt64 + r.i64,
    l.ts64 + r.i64,
    l.i64 + r.i64
FROM
    BigDates AS l
CROSS JOIN
    BigDates AS r
WHERE
    abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row
;
