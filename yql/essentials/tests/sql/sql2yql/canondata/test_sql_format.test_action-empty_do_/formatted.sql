/* syntax version 1 */
/* postgres can not */
DO
    EMPTY_ACTION()
;
$action1 = EMPTY_ACTION;
DO
    $action1()
;
$action2 = ($a, $_b) -> {
    RETURN $a;
};
DO
    $action2(12)
;
