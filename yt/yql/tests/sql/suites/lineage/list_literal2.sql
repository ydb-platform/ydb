USE plato;

$key = select min(key) from Input;

INSERT INTO Output
SELECT * FROM (
	SELECT $key as key
)
where key > '';

