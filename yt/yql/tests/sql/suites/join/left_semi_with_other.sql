PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data1 = (select cast(key as uint32) as key, subkey, value from Input1);
$data2 = (select cast(key as uint32) % 100u as key, subkey, value from Input2);

--INSERT INTO Output
SELECT
    i1.*
FROM $data1 as i1
LEFT SEMI JOIN $data2 as i2 ON i1.key = i2.key
LEFT OUTER JOIN $data1 as i3 ON i1.key = i3.key
ORDER BY i1.key
;
