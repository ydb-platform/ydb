/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output
SELECT
    x
FROM (
    SELECT
        ListFromRange(0, 100) AS x
)
    FLATTEN BY x
ORDER BY
    x
;
COMMIT;

SELECT
    *
FROM
    Output
WHERE
    x == 50
;
