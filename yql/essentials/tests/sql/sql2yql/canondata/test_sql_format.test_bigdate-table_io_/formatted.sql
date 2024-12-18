/* postgres can not */
USE plato;

SELECT
    *
FROM
    BigDates
ORDER BY
    row
;

INSERT INTO @Output WITH truncate
SELECT
    *
FROM
    BigDates
WHERE
    row > -100
ORDER BY
    row
;

COMMIT;

SELECT
    *
FROM
    @Output
ORDER BY
    row
;

SELECT
    row,
    CAST(d32 AS string),
    CAST(dt64 AS string),
    CAST(ts64 AS string),
    CAST(i64 AS string)
FROM
    BigDates
ORDER BY
    row
;
