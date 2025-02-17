--!syntax_pg
select * from (select 1 as x) a where x = (select 1);
