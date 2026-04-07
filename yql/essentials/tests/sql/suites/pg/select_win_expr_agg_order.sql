--!syntax_pg
select sum(x+1 + sum(z)) over w as l from (
  values (1,2,3),(1,5,3)) a(x,y,z)
group by x+1,y
window w as (
   order by x+1 + sum(z)
)
order by l
