/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    t.*, 
    COUNT(*) OVER (PARTITION BY key || "1") as c1,
    COUNT(*) OVER (PARTITION BY key || "2") as c2,
    COUNT(*) OVER w as c3,
FROM Input as t
WINDOW w AS (PARTITION BY key || "3")
ORDER BY subkey;
