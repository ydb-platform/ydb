/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output
SELECT
    *
FROM
    Output
;

INSERT INTO Output
SELECT
    *
FROM
    Output
;

COMMIT;

SELECT
    count(*)
FROM
    Output
;

INSERT INTO Output
SELECT
    *
FROM
    Output
;

INSERT INTO Output
SELECT
    *
FROM
    Output
;

COMMIT;

SELECT
    count(*)
FROM
    Output
;
