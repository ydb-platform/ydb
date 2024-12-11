/* postgres can not */
/* kikimr can not - no refs */
USE plato;

PRAGMA ResultSizeLimit = "25";

SELECT
    key || "a" AS key
FROM
    Input
ORDER BY
    key
;

SELECT
    key || "b" AS key
FROM
    Input
ORDER BY
    key
;

SELECT
    1 + 1
;

SELECT
    2 + 2
;
