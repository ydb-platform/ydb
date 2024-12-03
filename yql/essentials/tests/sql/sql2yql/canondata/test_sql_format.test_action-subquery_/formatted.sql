/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE SUBQUERY $q($name, $a) AS
    $i = (
        SELECT
            *
        FROM $name
    );
    $b = "_foo";

    SELECT
        key || $a || $b AS key
    FROM $i;
END DEFINE;

$z = (
    SELECT
        key
    FROM $q("Input", "_bar")
);

SELECT
    $z;

SELECT
    key
FROM $q("Input", "_baz")
ORDER BY
    key;

DEFINE SUBQUERY $e() AS
    SELECT
        "hello";
END DEFINE;

PROCESS $e();
