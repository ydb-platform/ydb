USE plato;

$input =
    SELECT
        *
    FROM
        AS_TABLE([<|a: "foo", b: "123"|>])
;

$mapping =
    SELECT
        {"a": "String", "b": "Int32"}
    FROM
        Input
    LIMIT 1;

$transformer = ($type) -> {
    $t = EvaluateType(ParseTypeHandle($type));
    RETURN ($value) -> {
        RETURN CAST($value AS $t);
    };
};

$converter = ($row) -> {
    RETURN EvaluateCode(
        LambdaCode(
            ($rowCode) -> {
                RETURN FuncCode(
                    "AsStruct", ListMap(
                        StructMembers($row), ($name) -> {
                            RETURN ListCode(
                                AtomCode($name),
                                FuncCode("Apply", QuoteCode($transformer(Unwrap($mapping[$name]))), FuncCode("Member", $rowCode, AtomCode($name)))
                            );
                        }
                    )
                );
            }
        )
    )($row);
};

SELECT
    *
FROM (
    SELECT
        $converter(TableRow())
    FROM
        $input
)
    FLATTEN COLUMNS
;
