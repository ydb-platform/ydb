--!syntax_pg
select coalesce(1,2),coalesce(x,2),coalesce(x,y,z) from 
(
select null::int4 as x, null::int4 as y, 6 as z
) a