/* syntax version 1 */
/* postgres can not */
SELECT
    EvaluateCode(FuncCode('Int32', AtomCode('1')))
;

$inc = EvaluateCode(
    LambdaCode(
        ($x) -> {
            RETURN FuncCode('+', $x, FuncCode('Int32', AtomCode('1')));
        }
    )
);

SELECT
    $inc(1)
;

$addPrefixForMembers = ($strValue) -> {
    $code = EvaluateCode(
        LambdaCode(
            ($str) -> {
                $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
                $list = ListMap(
                    $members, ($x) -> {
                        RETURN ListCode(AtomCode('prefix' || $x.Name), FuncCode('Member', $str, AtomCode($x.Name)));
                    }
                );
                RETURN FuncCode('AsStruct', $list);
            }
        )
    );
    RETURN $code($strValue);
};

SELECT
    $addPrefixForMembers(AsStruct(1 AS foo, '2' AS bar))
;
