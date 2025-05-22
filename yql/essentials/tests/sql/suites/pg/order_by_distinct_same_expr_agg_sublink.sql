--!syntax_pg
select distinct x + 1/(select count(*) from pg_type) as y from (select 1 as x) a order by x + 1/(select count(*) from pg_type)
