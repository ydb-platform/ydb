--!syntax_pg
create view a(x,y) as select 1,2;
select * from a;
create or replace view a(x,y) as values (3,4);
select * from a;
drop view a;
drop view if exists a,b,c;
