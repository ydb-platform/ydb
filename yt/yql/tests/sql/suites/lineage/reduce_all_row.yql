USE plato;

$r = ($s) -> (Yql::FlatMap($s, ($x)->($x.1)));

INSERT INTO Output
REDUCE Input
ON key USING ALL $r(TableRow());


