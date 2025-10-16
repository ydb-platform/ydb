--!syntax_pg
select -x as x from (values (1),(2)) u(x) order by x desc
