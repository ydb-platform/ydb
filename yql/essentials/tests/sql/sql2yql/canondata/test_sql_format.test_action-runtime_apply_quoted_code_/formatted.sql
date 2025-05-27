/* syntax version 1 */
/* postgres can not */
$func = ($x) -> {
    RETURN $x == 1;
};

$structApply = ($strValue, $f) -> {
    $code = EvaluateCode(
        LambdaCode(
            ($strCode) -> {
                $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
                RETURN Yql::Fold(
                    $members, ReprCode(FALSE), ($item, $state) -> {
                        $member = FuncCode('Member', $strCode, AtomCode($item.Name));
                        $apply = FuncCode('Apply', QuoteCode($f), $member);
                        RETURN FuncCode('Or', $state, $apply);
                    }
                );
            }
        )
    );
    RETURN $code($strValue);
};

SELECT
    $structApply(AsStruct(1 AS a, 2 AS b, 3 AS c), $func)
;

SELECT
    $structApply(AsStruct(4 AS a, 2 AS b, 3 AS c), $func)
;
