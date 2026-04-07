--!syntax_pg
select * from (values (1,2),(3,4)) as a(x,y)
right join (values (1,2),(2,5)) as b(u,v)
on a.x = 1;

select * from (values (1,2),(3,4)) as a(x,y)
right join (values (1,2),(2,5)) as b(u,v)
on a.x = 0;

select * from (values (1,2),(3,4)) as a(x,y)
right join (values (1,2),(2,5)) as b(u,v)
on b.u = 1;

select * from (values (1,2),(3,4)) as a(x,y)
right join (values (1,2),(2,5)) as b(u,v)
on b.u = 0;
