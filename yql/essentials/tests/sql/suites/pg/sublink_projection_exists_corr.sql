--!syntax_pg
select exists (select 1 where 1 = x) from (values (1,2),(2,3)) a(x,y);