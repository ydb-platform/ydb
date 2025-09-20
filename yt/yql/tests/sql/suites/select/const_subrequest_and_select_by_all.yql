/* postgres can not */
USE plato;

$input = (FROM Input SELECT key ORDER BY key LIMIT 3);

--insert into Output
SELECT * FROM $input;

SELECT * FROM Input WHERE key = $input;
