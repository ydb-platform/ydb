/* postgres can not */
USE plato;

$i = (
    SELECT
        cast(key as Double) as key,
        value
    FROM Input
    WHERE key < "100"
    ORDER BY key DESC
    LIMIT 1000
);

select distinct key
from $i
where value != ""
order by key;
