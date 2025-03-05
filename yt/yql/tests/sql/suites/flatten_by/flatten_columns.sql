/* postgres can not */
USE plato;

SELECT * FROM (
SELECT
    AsStruct(key as key, subkey as subkey),
    AsStruct("value: " || value as value)
FROM Input
)
FLATTEN COLUMNS;
