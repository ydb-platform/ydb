/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO @tmp
SELECT
    *
FROM Input
WHERE key > "100";
COMMIT;

INSERT INTO Output
SELECT
    *
FROM @tmp
WHERE key != "150";
DROP TABLE @tmp;
COMMIT;

INSERT INTO @tmp
SELECT
    *
FROM Input
WHERE key > "200";
COMMIT;

INSERT INTO Output
SELECT
    *
FROM @tmp
WHERE key != "150";
DROP TABLE @tmp;
COMMIT;
