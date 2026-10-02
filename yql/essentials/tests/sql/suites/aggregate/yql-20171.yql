SELECT Percentile(a.x, 0.50), Percentile(a.x, 0.75), Percentile(a.x, 1.00)
FROM (VALUES (1), (2), (3), (4), (5)) AS a(x);

SELECT Median(a.x), Median(b.y)
FROM (SELECT  1 AS x) AS a
JOIN (SELECT 10 AS y) AS b ON a.x == (b.y / 10);

SELECT Median(a.x), Median(b.x)
FROM (SELECT  1 AS x) AS a
JOIN (SELECT 10 AS x) AS b ON a.x == (b.x / 10);
