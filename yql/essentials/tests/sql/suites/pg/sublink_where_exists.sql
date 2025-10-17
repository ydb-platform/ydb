--!syntax_pg
select * from (select 1 as x) a where exists (select 1,2);
select * from (select 1 as x) a where exists (select 1,2 limit 0);
