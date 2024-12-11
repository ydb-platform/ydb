/* postgres can not */
USE plato;

SELECT
    *
FROM
    Input
WHERE
    key IN COMPACT (
        SELECT DISTINCT
            key
        FROM
            Input1
    )
ORDER BY
    key
;
