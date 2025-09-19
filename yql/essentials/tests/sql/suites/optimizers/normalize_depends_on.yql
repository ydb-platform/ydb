PRAGMA config.flags("NormalizeDependsOn");

$input = ListMap(ListFromRange(1ul, 6ul), ($i) -> (AsStruct($i AS key)));

$calc = ($row) -> {
    $inner = ListMap(ListFromRange(1ul, 6ul), ($y) -> (RandomNumber($y, $row)));
    return ListMap(ListFromRange(1ul, 6ul), ($x) -> (RandomNumber(AsTuple($x, $inner))));
};

SELECT $calc(TableRow()) FROM AS_TABLE($input);
