USE plato;

$stream =
    PROCESS InputView;

$type = EvaluateType(TypeHandle(ListItemType(TypeOf($stream))));
$lambda = ($row) -> (CAST($row AS $type));

PROCESS InputView
USING $lambda(TableRow());
