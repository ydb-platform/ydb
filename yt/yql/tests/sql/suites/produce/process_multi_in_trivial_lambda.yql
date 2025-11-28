/* syntax version 1 */
/* postgres can not */
USE plato;

$lambda = ($x) -> { return $x; };

$result = PROCESS Input, Input
USING
    $lambda(TableRow())
;

SELECT * FROM AS_TABLE($result.0);
