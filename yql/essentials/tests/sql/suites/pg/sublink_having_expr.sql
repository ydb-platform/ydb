--!syntax_pg
select 1 from (select 1 as x) a having min(x) = (select 1);
