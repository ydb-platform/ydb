--!syntax_pg
select x + 1 as y, x + 1 as z from 
    (select 1 x) a
order by x+1
-- [2,2]