/* postgres can not */
USE plato;

SELECT
    l.row,
    r.row,
    1,
    l.d32 - r.d,
    l.d32 - r.dt,
    l.d32 - r.ts,
    2,
    l.dt64 - r.d,
    l.dt64 - r.dt,
    l.dt64 - r.ts,
    3,
    l.ts64 - r.d,
    l.ts64 - r.dt,
    l.ts64 - r.ts
FROM
    BigDates AS l
CROSS JOIN
    NarrowDates AS r
WHERE
    abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row
;

SELECT
    l.row,
    r.row,
    1,
    l.d - r.d32,
    l.d - r.dt64,
    l.d - r.ts64,
    l.d - r.i64,
    l.d + r.i64,
    2,
    l.dt - r.d32,
    l.dt - r.dt64,
    l.dt - r.ts64,
    l.dt - r.i64,
    l.dt + r.i64,
    3,
    l.ts - r.d32,
    l.ts - r.dt64,
    l.ts - r.ts64,
    l.ts - r.i64,
    l.ts + r.i64
FROM
    NarrowDates AS l
CROSS JOIN
    BigDates AS r
WHERE
    abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row
;

SELECT
    l.row,
    r.row,
    1,
    l.d32 - r.i,
    l.dt64 - r.i,
    l.ts64 - r.i,
    l.i64 - r.i,
    2,
    l.d32 + r.i,
    l.dt64 + r.i,
    l.ts64 + r.i,
    l.i64 + r.i
FROM
    BigDates AS l
CROSS JOIN
    NarrowInterval AS r
WHERE
    abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row
;
