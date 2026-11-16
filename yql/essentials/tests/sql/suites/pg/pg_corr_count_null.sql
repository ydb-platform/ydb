--!syntax_pg
SELECT y,
    (SELECT count(*) FROM (VALUES (1), (2)) AS a(x)
     WHERE a.x = b.y OR b.y IS NULL) AS matched
FROM (VALUES (NULL::integer), (1), (4)) AS b(y)
ORDER BY y NULLS FIRST;
