--!syntax_pg
select * from (select 1 as x, 2 as w) as a
, (select 1 as y, 2 as v) as b
, (select 1 as z) as c 
where 
a.x = c.z
and 
a.x = b.y
and 
a.w = b.v