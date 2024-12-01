--!syntax_pg
select x, min(x) from  (select 1 as x) a group by x