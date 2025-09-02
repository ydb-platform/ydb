/* syntax version 1 */
/* postgres can not */
DEFINE ACTION $action($a, $b?) AS
    SELECT
        $a + ($b ?? 0)
    ;
END DEFINE;

DO
    $action(1)
;

DO
    $action(2, 3)
;
