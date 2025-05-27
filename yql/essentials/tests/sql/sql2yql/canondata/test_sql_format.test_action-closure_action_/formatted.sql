/* syntax version 1 */
/* postgres can not */
DEFINE ACTION $action($b, $c) AS
    $d = $b + $c;

    SELECT
        $b
    ;

    SELECT
        $c
    ;

    SELECT
        $d
    ;
END DEFINE;

DEFINE ACTION $closure_action($a) AS
    DO
        $a(3, 4)
    ;
END DEFINE;

DO
    $closure_action($action)
;
