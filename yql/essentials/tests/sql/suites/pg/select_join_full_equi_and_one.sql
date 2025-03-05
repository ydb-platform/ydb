--!syntax_pg
select * from (values (1,2),(3,4)) as a(x,y)
full join (values (1,2),(2,5)) as b(u,v)
on a.x = b.u and a.x=1;

select * from (values (1,2),(3,4)) as a(x,y)
full join (values (1,2),(2,5)) as b(u,v)
on a.x = b.u and b.u=1;