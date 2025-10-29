--!syntax_pg
select -x as y from (values (1),(2)) u(x) order by y
