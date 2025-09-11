/* postgres can not */
PRAGMA SimpleColumns;

USE plato;

$req = (SELECT 100500 as magic, t.* FROM Input as t);

--INSERT INTO Output
SELECT ff.*, subkey as sk, value as val FROM $req as ff ORDER BY sk
