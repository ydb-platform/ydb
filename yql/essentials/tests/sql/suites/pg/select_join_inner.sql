--!syntax_pg
select * from (select 1 as x) q join (select 1 as y) u on q.x=u.y;
select * from (select 1 as x) q join (select 2 as y) u on q.x=u.y;