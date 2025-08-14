--!syntax_pg
select * from (select 1 as x) q join (select 1 as y) u on q.x=u.y join (select 1 as z) v on v.z = q.x;
select * from (select 1 as x) q join (select 2 as y) u on q.x=u.y join (select 3 as z) v on v.z = q.x;
select * from (select 1 as x) q join (select 1 as y) u on q.x=u.y join (select 1 as z) v on v.z = u.y;
select * from (select 1 as x) q join (select 2 as y) u on q.x=u.y join (select 3 as z) v on v.z = u.y;