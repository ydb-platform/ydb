--!syntax_pg
WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION 
    SELECT 1 FROM t
)
SELECT * FROM t
