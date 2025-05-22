PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT * FROM (
SELECT
    AsStruct(key as key, subkey as subkey),
    AsStruct("value: " || value as value)
FROM Input1
) as a
FLATTEN COLUMNS
JOIN Input2
USING (key)
ORDER BY Input2.key;
