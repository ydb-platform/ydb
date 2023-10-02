--!syntax_pg
select avg(x), avg(y) from (values (1.0,2.0),(3.0,4.0)) as a(x,y)

