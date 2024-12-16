/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE ACTION $aaa($z) AS
    $table = $z.0;

    $k = (
        SELECT
            min(key || $z.1)
        FROM
            $table
    );

    DEFINE ACTION $bbb($n) AS
        SELECT
            $n || $k
        FROM
            $table
        ;
    END DEFINE;
    $ccc = EvaluateCode(QuoteCode($bbb));
    DO
        $ccc("1")
    ;
END DEFINE;

EVALUATE FOR $z IN AsList(AsTuple("Input", "foo"), AsTuple("Input", "bar")) DO
    $aaa($z)
;
