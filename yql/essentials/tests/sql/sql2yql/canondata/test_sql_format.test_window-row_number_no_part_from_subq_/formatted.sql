/* postgres can not */
USE plato;

SELECT
    key,
    ROW_NUMBER() OVER w AS row_num
FROM (
    SELECT
        *
    FROM
        Input
    WHERE
        key != "020"
)
WINDOW
    w AS ()
;
