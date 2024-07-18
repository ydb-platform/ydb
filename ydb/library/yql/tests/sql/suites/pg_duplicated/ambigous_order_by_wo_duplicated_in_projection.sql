--!syntax_pg
select z from 
    (select 1 x, 1 x, 3 z) a
order by x
-- column reference "x" is ambiguous