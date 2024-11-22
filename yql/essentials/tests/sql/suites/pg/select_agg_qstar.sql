--!syntax_pg
select 1,a.* from
(values (1,2,3)) a(x,y,z)
group by x,y,z