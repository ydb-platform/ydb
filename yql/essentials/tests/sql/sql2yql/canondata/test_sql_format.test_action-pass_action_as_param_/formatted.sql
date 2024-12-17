/* syntax version 1 */
/* postgres can not */
DEFINE ACTION $dup($x) AS
    DO
        $x()
    ;
    DO
        $x()
    ;
END DEFINE;

DO
    $dup(EMPTY_ACTION)
;

DEFINE ACTION $sel_foo() AS
    SELECT
        'foo'
    ;
END DEFINE;

DO
    $dup($sel_foo)
;
