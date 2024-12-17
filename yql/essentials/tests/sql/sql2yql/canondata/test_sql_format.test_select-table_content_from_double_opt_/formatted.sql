/* postgres can not */
/* syntax version 1 */
USE plato;

INSERT INTO @tmp WITH truncate
SELECT
    Just(Just(key)) AS key
FROM
    Input
;

COMMIT;

$key = (
    SELECT
        key
    FROM
        @tmp
);

SELECT
    *
FROM
    Input
WHERE
    key == $key
;
