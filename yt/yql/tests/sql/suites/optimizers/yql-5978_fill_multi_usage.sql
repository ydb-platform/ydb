/* postgres can not */
USE plato;

$ctl = (
    SELECT  1 AS join_col
            , MAX(key) AS max
    FROM Input
    );

INSERT INTO Output WITH TRUNCATE
SELECT * FROM $ctl;

$in = (
    SELECT  1 AS join_col
            , key
            , subkey
            , value
    FROM    Input
    );

SELECT 
        a.key AS key
        , a.subkey AS subkey
        , a.value AS value
FROM $in AS a
LEFT JOIN $ctl AS ctl
    USING (join_col)
WHERE key < max
;