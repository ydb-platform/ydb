--!syntax_pg
with foo(x) as (
   select 1
), bar as (select 2 as y from foo)
select x,b.y,z,a.y as y2 from foo,bar b,(
with foo as (select 3 as z)
select * from foo, bar) a
