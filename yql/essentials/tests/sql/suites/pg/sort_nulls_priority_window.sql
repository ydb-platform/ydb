--!syntax_pg
SELECT
x,array_agg(x) over (order by x nulls first)
FROM
(VALUES (null),(3),(1),(2)) a(x)
ORDER BY x nulls first;

SELECT
x,array_agg(x) over (order by x nulls last)
FROM
(VALUES (null),(3),(1),(2)) a(x)
ORDER BY x nulls last;

