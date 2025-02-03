PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT * FROM (
SELECT
    AsStruct(key as key, subkey as subkey),
    AsStruct("value1: " || value as value)
FROM Input1
) as a
FLATTEN COLUMNS
JOIN (
SELECT
    AsStruct(key as key, subkey as subkey),
    AsStruct("value2: " || value as value)
FROM Input2
) as b
FLATTEN COLUMNS
USING (key)
ORDER BY a.key;
