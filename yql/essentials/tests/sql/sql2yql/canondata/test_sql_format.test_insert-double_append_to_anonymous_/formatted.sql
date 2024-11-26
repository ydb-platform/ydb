/* postgres can not */
USE plato;

INSERT INTO @tmp
SELECT
    1 AS id,
    "qwer" AS val;
COMMIT;

INSERT INTO @tmp
SELECT
    2 AS id,
    "asdf" AS val;
COMMIT;

SELECT
    *
FROM @tmp;
