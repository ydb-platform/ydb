/* kikimr can not - no refs */
/* postgres can not */
USE plato;
PRAGMA ResultRowsLimit = "2";

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
