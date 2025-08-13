--!syntax_pg
select distinct (x + 1) * 2 as y,(x + 1) * 3,min(z) - 1 from (select 1 as x,2 as z) a group by x + 1 order by (x + 1) * 2, (x + 1) * 3, min(z) - 1
