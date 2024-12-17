/* postgres can not */
/* syntax version 1 */
USE plato;

$a = (
    SELECT
        *
    FROM
        Input
);

SELECT
    count(*)
FROM
    $a
;

SELECT
    count(*)
FROM
    $a
WHERE
    key != '075'
;

SELECT
    *
FROM
    $a
WHERE
    key != '075'
;
