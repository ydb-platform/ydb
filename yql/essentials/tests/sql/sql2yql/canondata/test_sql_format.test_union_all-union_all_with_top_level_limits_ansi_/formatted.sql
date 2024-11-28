/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA AnsiOrderByLimitInUnionAll;

$foo =
    SELECT
        *
    FROM Input
    UNION ALL
    SELECT
        *
    FROM Input
    LIMIT 2;

$bar =
    SELECT
        *
    FROM Input
    UNION ALL
    (
        SELECT
            *
        FROM Input
        LIMIT 2
    );

SELECT
    *
FROM $foo
ORDER BY
    subkey;

SELECT
    *
FROM $bar
ORDER BY
    subkey;

SELECT
    1 AS key
UNION ALL
SELECT
    2 AS key
ASSUME ORDER BY
    key
INTO RESULT aaa;

DISCARD SELECT
    1 AS key
UNION ALL
SELECT
    2 AS key
ASSUME ORDER BY
    key;
