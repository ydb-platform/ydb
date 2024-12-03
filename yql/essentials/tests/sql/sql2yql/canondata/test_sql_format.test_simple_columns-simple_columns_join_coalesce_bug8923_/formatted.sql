/* syntax version 1 */
USE plato;
PRAGMA SimpleColumns;
-- fails with CoalesceJoinKeysOnQualifiedAll
PRAGMA DisableCoalesceJoinKeysOnQualifiedAll;

$foo =
    SELECT
        1 AS key,
        1 AS value1;

$bar =
    SELECT
        1l AS key,
        2 AS value2;

$baz =
    SELECT
        1l AS key,
        2 AS value3;

SELECT
    foo.*
FROM $foo
    AS foo
JOIN $bar
    AS bar
ON CAST(foo.key AS Int32) == bar.key
JOIN $baz
    AS baz
ON bar.key == baz.key;
