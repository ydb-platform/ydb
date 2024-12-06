/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE SUBQUERY $sub1($i, $j?) AS
    SELECT
        $i - 1,
        $j
    ;
END DEFINE;

DEFINE SUBQUERY $sub2($i, $j?) AS
    SELECT
        $i + 1,
        $j
    ;
END DEFINE;
$sub = EvaluateCode(If(1 > 2, QuoteCode($sub1), QuoteCode($sub2)));
$s = SubqueryExtendFor([1, 2, 3], $sub);

PROCESS $s();
