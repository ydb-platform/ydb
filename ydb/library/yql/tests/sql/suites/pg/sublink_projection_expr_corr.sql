--!syntax_pg
select (select x) from (values (1,2),(2,3)) a(x,y);
select (select x limit 0) from (values (1,2),(2,3)) a(x,y);