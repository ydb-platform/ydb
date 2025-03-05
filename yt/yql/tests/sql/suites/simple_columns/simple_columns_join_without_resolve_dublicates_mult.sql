/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (SELECT cast(cast(key as uint32)/100 as string) as key, key as kk, cast(subkey as uint32) * 10 as subkey, "data: " || value as value FROM Input WHERE cast(key as uint32)/100 < 5);

--INSERT INTO Output
SELECT
  Input.*,
  d.*,
  Input.value as valueFromInput,
  d.subkey as subkeyFromD
WITHOUT
  Input.value, d.subkey, d.key
FROM Input JOIN $data as d ON Input.subkey = d.key
ORDER BY key, value
;
