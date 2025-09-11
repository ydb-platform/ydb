/* postgres can not */
USE plato;
PRAGMA DisableSimpleColumns;

$req = (SELECT 100500 as magic, t.* FROM Input as t);

--INSERT INTO Output
SELECT `t.subkey` as sk, `t.value` as val FROM $req ORDER BY sk
