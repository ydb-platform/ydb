USE plato;

INSERT INTO @tmp
SELECT
    1 AS key,
    2.0f AS fl1,
    3.0f AS fl2,
    2.5 AS db1,
    1.5 AS db2,
    TRUE AS b1,
    FALSE AS b2,
    Date('2023-01-08') AS d1,
    Date('2023-01-05') AS d2,
    Datetime('2023-01-08T00:00:00Z') AS dt1,
    Datetime('2023-01-05T00:00:00Z') AS dt2,
    Timestamp('2023-01-08T00:00:00.000000Z') AS ts1,
    Timestamp('2023-01-05T00:00:00.000000Z') AS ts2,
    Interval('PT1M') AS i1,
    Interval('PT1H') AS i2,
;

COMMIT;

SELECT
    t.*,
    fl1 > fl2,
    fl1 > key,
    fl2 > 2.0,
    fl2 > db2,
    db1 > db2,
    db2 <= key,
    db1 > fl2,
    db2 > 1.0f,
    b1 < b2,
    b1 == TRUE,
    FALSE >= b2,
    d1 < d2,
    d1 == dt1,
    ts2 == d2,
    d1 >= Date('2023-01-01'),
    ts1 == d1,
    ts2 >= dt2,
    Timestamp('2023-01-05T00:00:00.000000Z') <= d2,
    i1 < i2,
    i2 > Interval('PT59M'),
FROM
    @tmp AS t
;
