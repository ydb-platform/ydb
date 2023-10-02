/* postgres can not */
/* multirun can not */
/* kikimr can not - table truncate */
USE plato;

INSERT INTO Output WITH TRUNCATE
SELECT *
FROM Input
ORDER BY key, length(value);

COMMIT;

SELECT * FROM Output
WHERE key != "";
