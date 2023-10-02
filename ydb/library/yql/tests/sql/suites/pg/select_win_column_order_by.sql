--!syntax_pg
select x,rank() over (order by y) rnk from (select 1 x,2 y) a