USE plato;

$data = select
    key, subkey, value
from Input
group by GROUPING SETS (
    (key, subkey),
    (subkey, value)
    );

INSERT INTO @tmp WITH TRUNCATE
SELECT
    b.value
FROM $data AS a
LEFT JOIN Input AS b
USING (key)
 