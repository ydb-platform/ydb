USE plato;

$r = ($_key, $s) -> ($s);

INSERT INTO Output
REDUCE Input
ON key USING $r(TableRow());
