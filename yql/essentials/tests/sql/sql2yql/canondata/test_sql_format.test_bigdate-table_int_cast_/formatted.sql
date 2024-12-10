/* postgres can not */
USE plato;

SELECT
    row,
    1,
    CAST(i8 AS date32),
    CAST(i8 AS datetime64),
    CAST(i8 AS timestamp64),
    CAST(i8 AS interval64),
    2,
    CAST(i16 AS date32),
    CAST(i16 AS datetime64),
    CAST(i16 AS timestamp64),
    CAST(i16 AS interval64),
    3,
    CAST(i32 AS date32),
    CAST(i32 AS datetime64),
    CAST(i32 AS timestamp64),
    CAST(i32 AS interval64),
    4,
    CAST(i64 AS date32),
    CAST(i64 AS datetime64),
    CAST(i64 AS timestamp64),
    CAST(i64 AS interval64)
FROM
    Signed
ORDER BY
    row
;

SELECT
    row,
    1,
    CAST(d32 AS int8),
    CAST(d32 AS int16),
    CAST(d32 AS int32),
    CAST(d32 AS int64),
    2,
    CAST(dt64 AS int8),
    CAST(dt64 AS int16),
    CAST(dt64 AS int32),
    CAST(dt64 AS int64),
    3,
    CAST(ts64 AS int8),
    CAST(ts64 AS int16),
    CAST(ts64 AS int32),
    CAST(ts64 AS int64),
    4,
    CAST(i64 AS int8),
    CAST(i64 AS int16),
    CAST(i64 AS int32),
    CAST(i64 AS int64)
FROM
    BigDates
ORDER BY
    row
;

SELECT
    row,
    1,
    CAST(d32 AS uint8),
    CAST(d32 AS uint16),
    CAST(d32 AS uint32),
    CAST(d32 AS uint64),
    2,
    CAST(dt64 AS uint8),
    CAST(dt64 AS uint16),
    CAST(dt64 AS uint32),
    CAST(dt64 AS uint64),
    3,
    CAST(ts64 AS uint8),
    CAST(ts64 AS uint16),
    CAST(ts64 AS uint32),
    CAST(ts64 AS uint64),
    4,
    CAST(i64 AS uint8),
    CAST(i64 AS uint16),
    CAST(i64 AS uint32),
    CAST(i64 AS uint64)
FROM
    BigDates
ORDER BY
    row
;

SELECT
    row,
    1,
    CAST(ui8 AS date32),
    CAST(ui8 AS datetime64),
    CAST(ui8 AS timestamp64),
    CAST(ui8 AS interval64),
    2,
    CAST(ui16 AS date32),
    CAST(ui16 AS datetime64),
    CAST(ui16 AS timestamp64),
    CAST(ui16 AS interval64),
    3,
    CAST(ui32 AS date32),
    CAST(ui32 AS datetime64),
    CAST(ui32 AS timestamp64),
    CAST(ui32 AS interval64),
    4,
    CAST(ui64 AS date32),
    CAST(ui64 AS datetime64),
    CAST(ui64 AS timestamp64),
    CAST(ui64 AS interval64)
FROM
    Unsigned
ORDER BY
    row
;
