/* postgres can not */
/* syntax version 1 */
USE plato;

INSERT INTO @tmp WITH truncate
SELECT
    key
FROM
    Input
ORDER BY
    key DESC
;

COMMIT;

$key =
    SELECT
        key
    FROM
        @tmp
;

SELECT
    *
FROM
    Input
WHERE
    key == $key
;
