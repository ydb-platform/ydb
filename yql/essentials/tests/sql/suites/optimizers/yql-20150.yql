$table = SELECT * from AS_TABLE([<|key: 1, value: "2", subvalue: 3|>,<|key: 4, value: "5", subvalue: 6|>]);

$udf2 = ($stream) -> (
    Yql::Map(
        $stream,
        ($item) -> (
            ChooseMembers(
                $item,
                [
                    'key',
                    'value'
                ]
            )
        )
    )
);

$intermediate_table = (
    PROCESS $table
);

$udf = ($stream) -> (
    Yql::Map($stream, ($item) -> (AsStruct(
                                    $item.key * 2 as key,
                                    $item.value || ";" as value
                                    )))
);

$next_table = (
    PROCESS $intermediate_table
    USING $udf(TableRows())
);

PROCESS $next_table
USING $udf2(TableRows());


