--!syntax_pg
select * from (values (1,2),(2,3)) a(x,y) where 1 = all (select 1 where x = 1);
select * from (values (1,2),(2,3)) a(x,y) where 1 = all (select 1 where x = 10);
select * from (values (1,2),(2,3)) a(x,y) where 1 = all (select 2 where x = 1);
