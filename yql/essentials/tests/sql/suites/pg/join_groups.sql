--!syntax_pg
select 1 from
(select 1 as foo) a, (select 2 as foo) b
left join 
(select 1 as bar) c
on b.foo=c.bar