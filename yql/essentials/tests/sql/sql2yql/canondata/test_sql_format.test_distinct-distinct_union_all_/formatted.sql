/* syntax version 1 */
USE plato;

PRAGMA AnsiOrderByLimitInUnionAll;

SELECT DISTINCT
    key,
    value
FROM
    Input2
UNION ALL
SELECT
    key,
    value
FROM
    Input2
ORDER BY
    key,
    value
;
