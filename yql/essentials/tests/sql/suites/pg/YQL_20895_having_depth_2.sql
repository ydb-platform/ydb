--!syntax_pg

SELECT x
FROM (VALUES (1)) AS t(x) GROUP BY x
HAVING (Count(*) / (SELECT Count(*) FROM (VALUES ('1')) AS t(x))) = 1;
