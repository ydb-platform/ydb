/* postgres can not */
PRAGMA SimpleColumns;

USE plato;

$req = (SELECT 100500 as magic, t.* FROM Input as t);

--INSERT INTO Output
SELECT 
  ff.*,
  subkey as magic, -- 'magic' is exist from ff.magic
  value as val
FROM $req as ff ORDER BY sk
