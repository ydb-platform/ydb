PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data = (SELECT key as kk, subkey as sk, value FROM Input WHERE cast(key as uint32)/100 < 5);

--INSERT INTO Output
SELECT
  *
WITHOUT
  d.value
FROM Input JOIN $data as d ON Input.subkey = cast(cast(d.kk as uint32)/100 as string)
ORDER BY key
;
