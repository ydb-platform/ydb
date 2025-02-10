/* postgres can not */
PRAGMA SimpleColumns;
USE plato;

$data = (SELECT key, subkey as sk, value FROM Input WHERE cast(key as uint32)/100 < 5);

--INSERT INTO Output
SELECT
  d.*,
  subkey
FROM Input JOIN $data as d ON Input.key = d.key and Input.value == d.value
ORDER BY key, value
;
