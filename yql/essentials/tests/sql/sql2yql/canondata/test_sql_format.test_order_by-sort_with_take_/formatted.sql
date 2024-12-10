/* postgres can not */
USE plato;

INSERT INTO Output WITH truncate
SELECT
    *
FROM (
    SELECT
        *
    FROM
        Input
    LIMIT 3
)
ORDER BY
    key
;
