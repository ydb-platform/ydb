--!syntax_pg
select * from (values (1,2),(3,4)) as a(x,y)
inner join (values (1,2),(2,5)) as b(u,v)
on a.x = b.u and 1+1=2;

select * from (values (1,2),(3,4)) as a(x,y)
inner join (values (1,2),(2,5)) as b(u,v)
on a.x = b.u and 1+1=3;
