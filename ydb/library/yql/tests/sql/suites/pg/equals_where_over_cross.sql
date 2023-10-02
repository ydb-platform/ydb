--!syntax_pg
select * from (select 1 as x) as a
, (select 1 as y) as b
, (select 1 as z) as c 
where 
a.x = c.z
and 
a.x = b.y