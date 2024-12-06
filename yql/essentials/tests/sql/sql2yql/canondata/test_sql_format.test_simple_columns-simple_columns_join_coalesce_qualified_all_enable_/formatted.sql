/* syntax version 1 */
USE plato;

PRAGMA SimpleColumns;
PRAGMA CoalesceJoinKeysOnQualifiedAll;

$foo =
    SELECT
        1 AS key,
        1 AS value1
;

$bar =
    SELECT
        1l AS key,
        2 AS value2
;

SELECT
    foo.*
FROM
    $foo AS foo
JOIN
    $bar AS bar
ON
    foo.key == bar.key
;
-- output key has type Int64

