USE plato;

INSERT INTO @tmp
SELECT * FROM AS_TABLE([
	    <|key: <|field1: 1, field2: 1|>, value: 1|>,
	    <|key: <|field1: 1, field2: 1|>, value: 2|>,
	]);

COMMIT;

$reducer = ($_key, $stream) -> ($stream);

REDUCE @tmp
ON key
USING $reducer(TableRow());

