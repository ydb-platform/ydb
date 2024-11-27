--!syntax_pg
select 1 = all (select 2 where 1 = x) from (values (1,2),(2,3)) a(x,y);