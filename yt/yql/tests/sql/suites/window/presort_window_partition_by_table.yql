/* postgres can not */
use plato;
SELECT
   key, row_number() over w
FROM (SELECT AsList(key) as key, value from Input)
WINDOW w AS (partition by key order by value)
ORDER BY key;
