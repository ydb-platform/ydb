/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output
SELECT
    *
FROM
    plato.Input
WHERE
    value != "111"
LIMIT 3;

COMMIT;

INSERT INTO Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    value
;
