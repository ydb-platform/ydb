--!syntax_pg
select b.*,a.* from (select 1 as y) a, (select 2 as x) b
