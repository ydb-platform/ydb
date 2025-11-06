--!syntax_pg
select * from (select 10 as x) a where x > any (values (1),(2));
select * from (select 2 as x) a where x > any (values (1),(2));
select * from (select 1 as x) a where x > any (select 1 limit 0);
