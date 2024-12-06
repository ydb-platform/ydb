/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA AnsiOrderByLimitInUnionAll;

SELECT
    *
FROM
    Input
UNION ALL
SELECT
    *
FROM
    Input
INTO RESULT aaa;

DISCARD SELECT
    *
FROM
    Input
UNION ALL
SELECT
    *
FROM
    Input
;
