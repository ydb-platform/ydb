USE plato;

PRAGMA warning("disable", "4510");

$udf = ($_key, $stream) -> {
    $init = ($item) -> (AsStruct(1u AS cnt, $item AS row));
    $switch = ($_item, $_state) -> (FALSE);
    $update = ($item, $state) -> (
        AsStruct(
            $state.cnt + 1u AS cnt,
            if(($item.value > $state.row.value) ?? FALSE, $item, $state.row) AS row
        )
    );
    $state = YQL::Collect(YQL::Condense1($stream, $init, $switch, $update));
    RETURN $state;
};

REDUCE CONCAT(Input1, Input2)
PRESORT
    subkey
ON
    key
USING $udf(TableRow());
