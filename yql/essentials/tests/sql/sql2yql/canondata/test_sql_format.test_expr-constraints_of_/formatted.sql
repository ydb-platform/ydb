/* syntax version 1 */
/* postgres can not */
PRAGMA warning("disable", "4510");
USE plato;

$foo =
    SELECT
        subkey,
        key,
        value AS v
    FROM
        Input
    ORDER BY
        subkey ASC,
        key DESC
    LIMIT 0;

$x =
    PROCESS $foo;

SELECT
    YQL::ConstraintsOf($x) AS constraints
;
