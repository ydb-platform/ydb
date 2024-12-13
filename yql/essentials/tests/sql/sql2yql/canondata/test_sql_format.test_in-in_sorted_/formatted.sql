/* postgres can not */
/* syntax version 1 */
USE plato;

$in =
    SELECT
        key
    FROM
        InputSorted
    WHERE
        key < '100'
;

SELECT
    *
FROM
    InputSorted
WHERE
    key IN $in
;
