/* syntax version 1 */
/* postgres can not */
USE plato;

$foo = (
    SELECT
        100500 AS bar
);

SELECT
    bar
FROM
    $foo
;
