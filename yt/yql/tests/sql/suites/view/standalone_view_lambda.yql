use plato;

$stream = process InputView;
$type = EvaluateType(TypeHandle(ListItemType(TypeOf($stream))));

$lambda = ($row) -> (Cast($row as $type));

process InputView using $lambda(TableRow());
