--!syntax_pg
SELECT y,
(
    select x+y as v from (values (3),(1),(2)) a(x)
    order by v
    limit 1
)
FROM
(values (40),(50),(60)) b(y)
order by y

