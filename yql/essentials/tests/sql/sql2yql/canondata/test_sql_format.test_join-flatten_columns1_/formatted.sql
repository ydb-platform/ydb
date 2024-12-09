PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        AsStruct(key AS key, subkey AS subkey),
        AsStruct("value: " || value AS value)
    FROM Input1
)
    AS a
    FLATTEN COLUMNS
JOIN Input2
USING (key)
ORDER BY
    Input2.key;
