/* postgres can not */
/* syntax version 1 */
select FormatCode(
    ListCode(
        AtomCode("1"),
        AsList(AtomCode("2"),AtomCode("3")),
        FuncCode("Func",
            AtomCode("4"),
            AsList(AtomCode("5"),AtomCode("6"))),
        LambdaCode(()->{ return AtomCode("7") }),
        LambdaCode(($x)->{ return FuncCode("-",$x) }),
        LambdaCode(($x,$y)->{ return FuncCode("*",$x,$y) }),
        LambdaCode(2, ($args)->{ return FuncCode("+",Unwrap($args[0]),Unwrap($args[1])) }),
    )
);
