--!syntax_pg
select * from 
(select 1 as foo) a
join
(select 2 as bar) b
on a.foo + 1= b.bar
join
((select 3 as x) c
join
(select 4 as y) d
on c.x=d.y-1) on a.foo=c.x-2 and b.bar=d.y-2