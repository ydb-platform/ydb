--!syntax_pg
select x,rank() over (partition by y) rnk from (select 1 x,2 y) a