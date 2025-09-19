--!syntax_pg
SELECT y,
(
    select x+y as v from (values (1),(1),(1)) a(x)
    limit 1
)
FROM
(values (40),(50),(60)) b(y)
order by y

