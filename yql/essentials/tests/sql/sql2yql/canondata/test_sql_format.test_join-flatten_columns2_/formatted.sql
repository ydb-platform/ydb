PRAGMA DisableSimpleColumns;

/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        AsStruct(key AS key, subkey AS subkey),
        AsStruct('value1: ' || value AS value)
    FROM
        Input1
) AS a
    FLATTEN COLUMNS
JOIN (
    SELECT
        AsStruct(key AS key, subkey AS subkey),
        AsStruct('value2: ' || value AS value)
    FROM
        Input2
) AS b
    FLATTEN COLUMNS
USING (key)
ORDER BY
    a.key
;
