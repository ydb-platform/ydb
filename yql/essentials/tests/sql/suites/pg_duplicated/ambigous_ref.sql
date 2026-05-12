--!syntax_pg
/* custom error:Column reference is ambiguous: x*/
select a.x from (select 1 x, 2 x) a
