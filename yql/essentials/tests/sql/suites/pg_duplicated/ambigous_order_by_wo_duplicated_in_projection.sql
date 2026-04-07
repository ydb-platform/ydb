--!syntax_pg
/* custom error:Column reference is ambiguous: x*/
select z from
    (select 1 x, 1 x, 3 z) a
order by x
-- column reference "x" is ambiguous
