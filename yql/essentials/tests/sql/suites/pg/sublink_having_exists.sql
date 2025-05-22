--!syntax_pg
select 1 from (select 1 as x) a having exists (select 1,2);
select 1 from (select 1 as x) a having exists (select 1,2 limit 0);
