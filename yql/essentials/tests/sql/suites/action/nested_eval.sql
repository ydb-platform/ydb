/* syntax version 1 */
/* postgres can not */
$make_struct = Callable(
    Callable<(String) -> Struct<lel : Int32>>,
    ($_string) -> { return AsStruct(5 as lel); }
);

$kekify_struct = ($struct) -> {
    return EvaluateCode(FuncCode("AsStruct",
        ListMap(
            StructTypeComponents(TypeHandle(TypeOf($struct))),
            ($_component) -> { return ListCode(AtomCode("kek"), ReprCode(42)); }
        )
    ));
};

$struct = AsStruct(
    Callable(
        Callable<(String)->Struct<kek : Int32>>,
        ($string) -> { return $kekify_struct($make_struct($string)); }
    ) as KekFromString,
    TypeHandle(Int32) as IntHandle
);

select FormatType(EvaluateType($struct.IntHandle));
