--!syntax_pg
select * from (select 10 as x) a where x in (values (1),(2));
select * from (select 2 as x) a where x in (values (1),(2));
select * from (select 1 as x) a where x in (select 1 limit 0);
