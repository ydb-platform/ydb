--!syntax_pg
/* custom error:Column reference is ambiguous: x*/
select a.* from
    (select 1 x, 2 x) a
order by x
-- ORDER BY "x" is ambiguous
