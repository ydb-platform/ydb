/* postgres can not */
/* multirun can not */
/* kikimr can not - table truncate */
USE plato;

INSERT INTO @a
SELECT *
FROM Input
WHERE key < "100"
ORDER BY key DESC;

COMMIT;

INSERT INTO Output
SELECT *
FROM @a
ORDER BY key DESC;

COMMIT;

SELECT *
FROM Output;