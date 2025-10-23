--!syntax_pg
select distinct x+1 as y,x-1 from (select 1 as x) a order by x+1,x-1

