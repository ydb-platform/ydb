USE plato;

$one = select dummy from as_table([<|dummy:1|>]);

INSERT INTO Output
SELECT key FROM Input AS a
CROSS JOIN $one AS b;

