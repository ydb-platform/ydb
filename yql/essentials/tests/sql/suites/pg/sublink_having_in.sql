--!syntax_pg
select 1 from (select 10 as x) a having min(x) in (values (1),(2));
select 1 from (select 2 as x) a having min(x) in (values (1),(2));
select 1 from (select 1 as x) a having min(x) in (select 1 limit 0);
