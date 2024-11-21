--!syntax_pg
select a.x + 1 as y, b.x + 1 as z from 
    ((select 1 x) a
    join
    (select 1 x) b
    on a.x = b.x)
order by x+1
-- column reference "x" is ambiguous