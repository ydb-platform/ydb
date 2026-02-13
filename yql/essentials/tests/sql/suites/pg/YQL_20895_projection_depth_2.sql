--!syntax_pg

SELECT (Count(*) / (SELECT Count(*) FROM (VALUES ('1')) AS t(x)))
FROM (VALUES (1)) AS t(x) GROUP BY x;
