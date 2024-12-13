/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    NULL AS x,
    1 AS y
;

COMMIT;

INSERT INTO @foo
SELECT
    2 AS y
;

COMMIT;

SELECT
    *
FROM
    @foo
;
