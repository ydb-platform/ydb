/* postgres can not */
SELECT
   key, row_number() over w
FROM (SELECT "a" as key, "z" as value)
WINDOW w AS (partition by key order by value);
