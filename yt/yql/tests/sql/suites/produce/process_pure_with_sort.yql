/* syntax version 1 */
/* postgres can not */
USE plato;

$sorted = ($world, $input, $orderByColumns, $asc) -> {
    $n = ListLength($orderByColumns);

    $keySelector = LambdaCode(($row) -> {
        $items = ListMap($orderByColumns,
            ($x) -> {
                RETURN FuncCode("Member", $row, AtomCode($x));
            });
        RETURN ListCode($items);
    });

    $sort = EvaluateCode(LambdaCode(($x) -> {
        return FuncCode("Sort",
            $x, 
            ListCode(ListReplicate(ReprCode($asc), $n)), 
            $keySelector) 
    }));

    RETURN $sort($input($world));
};

DEFINE SUBQUERY $source() AS
    PROCESS Input0;
END DEFINE;

PROCESS $sorted($source, AsList("key","subkey"), true);
PROCESS $sorted($source, AsList("value"), true);
PROCESS $sorted($source, ListCreate(TypeOf("")), true);