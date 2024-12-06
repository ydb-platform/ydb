/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO @tmpTable
SELECT
    date32('1969-12-31') AS d32,
    datetime64('1969-12-31T0:0:0Z') AS dt64,
    timestamp64('1969-12-31T0:0:0Z') AS ts64,
    interval64('P65536D') AS i64
;

COMMIT;

INSERT INTO Output
SELECT
    *
FROM
    @tmpTable
WHERE
    d32 < date32('1970-1-1')
;
