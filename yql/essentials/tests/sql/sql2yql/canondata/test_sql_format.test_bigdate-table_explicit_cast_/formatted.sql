/* postgres can not */
USE plato;

SELECT
    row,
    CAST(i AS interval64)
FROM NarrowInterval
ORDER BY
    row;

SELECT
    row,
    1,
    CAST(d AS date),
    CAST(d AS datetime),
    CAST(d AS timestamp),
    2,
    CAST(dt AS date),
    CAST(dt AS datetime),
    CAST(dt AS timestamp),
    3,
    CAST(ts AS date),
    CAST(ts AS datetime),
    CAST(ts AS timestamp)
FROM NarrowDates
ORDER BY
    row;

SELECT
    row,
    1,
    CAST(d AS date32),
    CAST(d AS datetime64),
    CAST(d AS timestamp64),
    2,
    CAST(dt AS date32),
    CAST(dt AS datetime64),
    CAST(dt AS timestamp64),
    3,
    CAST(ts AS date32),
    CAST(ts AS datetime64),
    CAST(ts AS timestamp64)
FROM NarrowDates
ORDER BY
    row;

SELECT
    row,
    1,
    CAST(d32 AS date),
    CAST(d32 AS datetime),
    CAST(d32 AS timestamp),
    2,
    CAST(dt64 AS date),
    CAST(dt64 AS datetime),
    CAST(dt64 AS timestamp),
    3,
    CAST(ts64 AS date),
    CAST(ts64 AS datetime),
    CAST(ts64 AS timestamp),
    4,
    CAST(i64 AS interval)
FROM BigDates
ORDER BY
    row;

SELECT
    row,
    1,
    CAST(d32 AS datetime64),
    CAST(d32 AS timestamp64),
    2,
    CAST(dt64 AS date32),
    CAST(dt64 AS timestamp64),
    3,
    CAST(ts64 AS date32),
    CAST(ts64 AS datetime64)
FROM BigDates
ORDER BY
    row;
