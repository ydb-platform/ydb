USE plato;

SELECT
    a,
    lag(a) over w as prev_a,
    min(a) over w as min_a
FROM Input
WINDOW w AS (PARTITION BY b ORDER by c)
ORDER BY a;
