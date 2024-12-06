/* syntax version 1 */
USE plato;

$foo =
    PROCESS Input;

SELECT
    ListSort(EvaluateExpr($foo), ($item) -> (AsTuple($item.key, $item.subkey)))
;
