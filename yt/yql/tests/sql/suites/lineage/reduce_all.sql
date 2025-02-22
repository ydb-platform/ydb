USE plato;

$r = ($s) -> ($s);

INSERT INTO Output
REDUCE Input
ON key USING ALL $r(TableRows());

