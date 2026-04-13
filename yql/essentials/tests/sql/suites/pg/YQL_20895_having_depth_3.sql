--!syntax_pg

SELECT x
FROM (VALUES (1)) AS t(x) GROUP BY x
HAVING (Count(*) / (
    SELECT (Count(*) / (SELECT Count(*) FROM (VALUES (1.0)) AS t(x)))
    FROM (VALUES ('1')) AS t(x) GROUP BY x
)) = 1;
