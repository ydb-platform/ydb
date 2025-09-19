--!syntax_pg
select row_number() over w as l from (
  values (1,2,3),(1,5,3)) a(x,y,z)
group by x+1,y
window w as (
  partition by x+1 + sum(z)
)
order by l
