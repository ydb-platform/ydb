--!syntax_pg
SELECT cume_dist() over w FROM (VALUES (4),(5),(5),(6)) as a(x)
window w as (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
