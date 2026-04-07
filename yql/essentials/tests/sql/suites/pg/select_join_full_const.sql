--!syntax_pg
select * from (values (1,2),(3,4)) as a(x,y)
full join (values (1,2),(2,5)) as b(u,v)
on 1+1=2;

select * from (select * from (values (1,2),(3,4)) as a(x,y) limit 0) a
full join (values (1,2),(2,5)) as b(u,v)
on 1+1=2;

select * from (values (1,2),(3,4)) as a(x,y)
full join (select * from (values (1,2),(2,5)) as b(u,v) limit 0) b
on 1+1=2;

select * from (select * from (values (1,2),(3,4)) as a(x,y) limit 0) a
full join (select * from (values (1,2),(2,5)) as b(u,v) limit 0) b
on 1+1=2;

select * from (values (1,2),(3,4)) as a(x,y)
full join (values (1,2),(2,5)) as b(u,v)
on 1+1=3;

select * from (select * from (values (1,2),(3,4)) as a(x,y) limit 0) a
full join (values (1,2),(2,5)) as b(u,v)
on 1+1=3;

select * from (values (1,2),(3,4)) as a(x,y)
full join (select * from (values (1,2),(2,5)) as b(u,v) limit 0) b
on 1+1=3;

select * from (select * from (values (1,2),(3,4)) as a(x,y) limit 0) a
full join (select * from (values (1,2),(2,5)) as b(u,v) limit 0) b
on 1+1=3;
