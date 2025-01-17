--!syntax_pg
select * from (select 1 as x, 3 as y) u
union all
select * from (select null::int4 as y, 2 as x) v
union all
select * from (select 4 as y, null::int4 as x) v
