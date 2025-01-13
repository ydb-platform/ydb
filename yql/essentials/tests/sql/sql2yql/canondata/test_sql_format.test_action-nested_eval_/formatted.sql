/* syntax version 1 */
/* postgres can not */
$make_struct = CALLABLE (
    Callable<(String) -> Struct<lel: Int32>>,
    ($_string) -> {
        RETURN AsStruct(5 AS lel);
    }
);

$kekify_struct = ($struct) -> {
    RETURN EvaluateCode(
        FuncCode(
            'AsStruct',
            ListMap(
                StructTypeComponents(TypeHandle(TypeOf($struct))),
                ($_component) -> {
                    RETURN ListCode(AtomCode('kek'), ReprCode(42));
                }
            )
        )
    );
};

$struct = AsStruct(
    CALLABLE (
        Callable<(String) -> Struct<kek: Int32>>,
        ($string) -> {
            RETURN $kekify_struct($make_struct($string));
        }
    ) AS KekFromString,
    TypeHandle(Int32) AS IntHandle
);

SELECT
    FormatType(EvaluateType($struct.IntHandle))
;
