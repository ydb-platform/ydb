--!syntax_pg
select distinct x from (values (1),(2),(1),(null::int4)) a(x)
