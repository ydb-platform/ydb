/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (SELECT key as kk, subkey as sk, "data: " || value as value FROM Input WHERE cast(key as uint32)/100 < 5);

--INSERT INTO Output
SELECT
  Input.*,
  d.*,
  Input.value as valueFromInput
WITHOUT
  Input.value
FROM Input JOIN $data as d ON Input.subkey = cast(cast(d.kk as uint32)/100 as string)
ORDER BY key, value
;
