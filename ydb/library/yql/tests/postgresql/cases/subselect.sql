-- Check grammar's handling of extra parens in assorted contexts
SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;
