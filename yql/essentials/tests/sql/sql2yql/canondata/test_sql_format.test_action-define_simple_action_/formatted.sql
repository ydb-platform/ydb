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

DO
    $action(1, 2)
;
