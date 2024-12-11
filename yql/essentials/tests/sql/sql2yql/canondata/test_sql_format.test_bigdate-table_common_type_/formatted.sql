/* postgres can not */
USE plato;

SELECT
    bd.row AS row,
    1,
    [d, dt],
    [d, ts],
    [d, d32],
    [d, dt64],
    [d, ts64],
    2,
    [dt, ts],
    [dt, d32],
    [dt, dt64],
    [dt, ts64],
    3,
    [ts, d32],
    [ts, dt64],
    [ts, ts64],
    4,
    [d32, dt64],
    [d32, ts64],
    5,
    [dt64, ts64]
FROM
    BigDates AS bd
JOIN
    NarrowDates
USING (row)
ORDER BY
    row
;

SELECT
    bd.row AS row,
    [i, i64]
FROM
    BigDates AS bd
JOIN
    NarrowInterval
USING (row)
ORDER BY
    row
;
