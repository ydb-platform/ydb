--!syntax_pg
SELECT
*
FROM
(VALUES (null),(3),(1),(2)) as a(x)
ORDER BY x nulls last;

SELECT
*
FROM
(VALUES (null),(3),(1),(2)) as a(x)
ORDER BY x nulls first;

