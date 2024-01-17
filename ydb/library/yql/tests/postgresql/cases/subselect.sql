--
-- SUBSELECT
--
SELECT 1 AS one WHERE 1 IN (SELECT 1);
SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);
SELECT 1 AS zero WHERE 1 IN (SELECT 2);
-- Check grammar's handling of extra parens in assorted contexts
SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;
