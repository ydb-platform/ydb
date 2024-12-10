/* syntax version 1 */
/* postgres can not */
DEFINE ACTION $action($b, $c) AS
    DEFINE ACTION $aaa() AS
        SELECT
            $b
        ;
    END DEFINE;

    DEFINE ACTION $bbb() AS
        SELECT
            $c
        ;
    END DEFINE;
    DO
        $aaa()
    ;
    DO
        $bbb()
    ;
END DEFINE;

DO
    $action(1, 2)
;
