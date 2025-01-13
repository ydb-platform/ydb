PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data = (SELECT key as kk, subkey as sk, value as val FROM Input WHERE cast(key as uint32)/100 > 3);

--INSERT INTO Output
SELECT
  *
FROM Input JOIN $data as d ON Input.subkey = d.kk
ORDER BY key, d.val
;
