/* postgres can not */
USE plato;

SELECT
    l.row,
    r.row,
    1,
    l.d32 - r.d32,
    l.d32 - r.dt64,
    l.d32 - r.ts64,
    2,
    l.dt64 - r.d32,
    l.dt64 - r.dt64,
    l.dt64 - r.ts64,
    3,
    l.ts64 - r.d32,
    l.ts64 - r.dt64,
    l.ts64 - r.ts64
FROM BigDates
    AS l
CROSS JOIN BigDates
    AS r
WHERE abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row;
