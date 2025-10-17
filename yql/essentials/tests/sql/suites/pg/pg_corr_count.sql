--!syntax_pg
SELECT y,
(select count(*) from (values (1),(2),(3)) a(x) where a.x=y
)
FROM
(values (4)) b(y)

