--!syntax_pg
select * from (select 1 as x ) u full join (select 1 as y) v on u.x=v.y full join (select 1 as z) r on v.y=r.z;
select * from (select 1 as x ) u full join (select 2 as y) v on u.x=v.y full join (select 3 as z) r on v.y=r.z;
