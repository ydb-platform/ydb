/* postgres can not */
/* multirun can not */
USE plato;

$out = (
SELECT 
    *
FROM 
    `Input`
WHERE
    value != "111"
);

$row_count = (
    SELECT 
        COUNT(*) 
    FROM 
        $out
);

$needed_row =  COALESCE(CAST(CAST($row_count as float) * 0.5 as Uint64), 1);

INSERT INTO Output WITH TRUNCATE 
SELECT 
    *
FROM $out ORDER BY key DESC
LIMIT 1 OFFSET $needed_row;

