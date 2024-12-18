/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        AsStruct(key AS key, subkey AS subkey),
        AsStruct('value: ' || value AS value)
    FROM
        Input
)
    FLATTEN COLUMNS
;
