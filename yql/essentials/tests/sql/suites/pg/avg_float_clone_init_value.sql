--!syntax_pg
select avg(x) ax, avg(y) ay from (values (1.0,2.0),(3.0,4.0)) as a(x,y)

