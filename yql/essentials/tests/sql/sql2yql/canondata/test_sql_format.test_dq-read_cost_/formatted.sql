PRAGMA warning("disable", "4510");
PRAGMA CostBasedOptimizer = "PG";
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
    LIMIT 10;

$x =
    PROCESS $foo;

SELECT
    YQL::CostsOf($x) AS costs
;
