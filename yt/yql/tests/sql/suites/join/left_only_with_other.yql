PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data1 = (select cast(key as uint32) as key, subkey, value from Input1);
$data2 = (select cast(key as uint32) % 100u as key, subkey, value from Input1);

--INSERT INTO Output
SELECT
    i1.*
FROM $data1 as i1
LEFT ONLY JOIN $data2 as i2 ON i1.key = i2.key
JOIN $data1 as i3 ON i1.key = i3.key
;
