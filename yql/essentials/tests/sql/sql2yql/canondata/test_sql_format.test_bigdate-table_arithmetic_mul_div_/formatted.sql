/* postgres can not */
USE plato;

SELECT
    l.row,
    r.row,
    1,
    l.i64 * i8,
    l.i64 * i16,
    l.i64 * i32,
    l.i64 * r.i64,
    2,
    i8 * l.i64,
    i16 * l.i64,
    i32 * l.i64,
    r.i64 * l.i64,
    3,
    l.i64 / i8,
    l.i64 / i16,
    l.i64 / i32,
    l.i64 / r.i64
FROM BigDates
    AS l
CROSS JOIN Signed
    AS r
WHERE abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row;

SELECT
    l.row,
    r.row,
    1,
    i64 * ui8,
    i64 * ui16,
    i64 * ui32,
    i64 * ui64,
    2,
    ui8 * i64,
    ui16 * i64,
    ui32 * i64,
    ui64 * i64,
    3,
    i64 / ui8,
    i64 / ui16,
    i64 / ui32,
    i64 / ui64
FROM BigDates
    AS l
CROSS JOIN Unsigned
    AS r
WHERE abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row;

SELECT
    l.row,
    r.row,
    1,
    l.i * i8,
    l.i * i16,
    l.i * i32,
    l.i * r.i64,
    2,
    i8 * l.i,
    i16 * l.i,
    i32 * l.i,
    r.i64 * l.i,
    3,
    l.i / i8,
    l.i / i16,
    l.i / i32,
    l.i / r.i64
FROM NarrowInterval
    AS l
CROSS JOIN Signed
    AS r
WHERE abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row;

SELECT
    l.row,
    r.row,
    1,
    i * ui8,
    i * ui16,
    i * ui32,
    i * ui64,
    2,
    ui8 * i,
    ui16 * i,
    ui32 * i,
    ui64 * i,
    3,
    i / ui8,
    i / ui16,
    i / ui32,
    i / ui64
FROM NarrowInterval
    AS l
CROSS JOIN Unsigned
    AS r
WHERE abs(l.row) <= 7 AND abs(r.row) <= 7
ORDER BY
    l.row,
    r.row;
