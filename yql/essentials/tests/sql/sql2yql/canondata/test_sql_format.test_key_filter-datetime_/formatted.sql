/* postgres can not */
/* syntax version 1 */
USE plato;

INSERT INTO @d
SELECT
    *
FROM
    as_table(AsList(<|key: Date('2021-01-03')|>))
ASSUME ORDER BY
    key
;

INSERT INTO @dt
SELECT
    *
FROM
    as_table(AsList(<|key: Datetime('2021-01-03T21:15:30Z')|>))
ASSUME ORDER BY
    key
;

INSERT INTO @ts
SELECT
    *
FROM
    as_table(AsList(<|key: Timestamp('2021-01-03T21:15:30.673521Z')|>))
ASSUME ORDER BY
    key
;

COMMIT;

SELECT
    *
FROM
    @d
WHERE
    key > Date('2021-01-02')
;

SELECT
    *
FROM
    @d
WHERE
    key >= Date('2021-01-03')
;

SELECT
    *
FROM
    @d
WHERE
    key < Date('2021-01-04')
;

SELECT
    *
FROM
    @d
WHERE
    key <= Date('2021-01-03')
;

SELECT
    *
FROM
    @d
WHERE
    key > Datetime('2021-01-02T23:00:00Z')
;

SELECT
    *
FROM
    @d
WHERE
    key >= Datetime('2021-01-02T23:00:00Z')
;

SELECT
    *
FROM
    @d
WHERE
    key < Datetime('2021-01-03T20:00:00Z')
;

SELECT
    *
FROM
    @d
WHERE
    key <= Datetime('2021-01-03T20:00:00Z')
;

SELECT
    *
FROM
    @d
WHERE
    key > Timestamp('2021-01-02T23:32:01.673521Z')
;

SELECT
    *
FROM
    @d
WHERE
    key >= Timestamp('2021-01-02T23:32:01.673521Z')
;

SELECT
    *
FROM
    @d
WHERE
    key < Timestamp('2021-01-03T00:00:00.673521Z')
;

SELECT
    *
FROM
    @d
WHERE
    key <= Timestamp('2021-01-03T00:00:00.673521Z')
;

-------------------------------------------
SELECT
    *
FROM
    @dt
WHERE
    key > Date('2021-01-03')
;

SELECT
    *
FROM
    @dt
WHERE
    key >= Date('2021-01-03')
;

SELECT
    *
FROM
    @dt
WHERE
    key < Date('2021-01-04')
;

SELECT
    *
FROM
    @dt
WHERE
    key <= Date('2021-01-04')
;

SELECT
    *
FROM
    @dt
WHERE
    key > Datetime('2021-01-03T21:15:29Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key >= Datetime('2021-01-03T21:15:30Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key < Datetime('2021-01-03T21:15:31Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key <= Datetime('2021-01-03T21:15:30Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key > Timestamp('2021-01-03T21:15:29.673521Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key >= Timestamp('2021-01-03T21:15:29.673521Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key < Timestamp('2021-01-03T21:15:30.673521Z')
;

SELECT
    *
FROM
    @dt
WHERE
    key <= Timestamp('2021-01-03T21:15:30.673521Z')
;

-------------------------------------------
SELECT
    *
FROM
    @ts
WHERE
    key > Date('2021-01-03')
;

SELECT
    *
FROM
    @ts
WHERE
    key >= Date('2021-01-03')
;

SELECT
    *
FROM
    @ts
WHERE
    key < Date('2021-01-04')
;

SELECT
    *
FROM
    @ts
WHERE
    key <= Date('2021-01-04')
;

SELECT
    *
FROM
    @ts
WHERE
    key > Datetime('2021-01-03T20:00:00Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key >= Datetime('2021-01-03T20:00:00Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key < Datetime('2021-01-03T22:00:00Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key <= Datetime('2021-01-03T22:00:00Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key > Timestamp('2021-01-03T21:15:30.573521Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key >= Timestamp('2021-01-03T21:15:30.673521Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key < Timestamp('2021-01-03T21:15:30.773521Z')
;

SELECT
    *
FROM
    @ts
WHERE
    key <= Timestamp('2021-01-03T21:15:30.673521Z')
;
