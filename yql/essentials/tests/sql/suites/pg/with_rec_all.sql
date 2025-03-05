--!syntax_pg
WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM t WHERE n < 5
)
SELECT * FROM t

