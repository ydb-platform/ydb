--!syntax_pg
select x + 1 as y, x - 1 as y from 
    (select 1 x) a
order by y
-- order by y is ambigous