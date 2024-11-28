/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output WITH truncate
SELECT
    CAST(value AS tzdatetime) AS x
FROM Input
ORDER BY
    x DESC;
COMMIT;

SELECT
    *
FROM Output;
