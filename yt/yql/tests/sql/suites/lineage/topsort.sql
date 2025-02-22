USE plato;

INSERT INTO Output
SELECT a.value
FROM Input AS a
INNER JOIN (SELECT value FROM Input ORDER BY value LIMIT 1) AS b
USING (value);

