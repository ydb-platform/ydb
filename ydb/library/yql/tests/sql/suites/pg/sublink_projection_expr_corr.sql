--!syntax_pg
select (select x) as z from (values (1,2),(2,3)) a(x,y) order by z;
select (select x limit 0) as z from (values (1,2),(2,3)) a(x,y) order by z;

