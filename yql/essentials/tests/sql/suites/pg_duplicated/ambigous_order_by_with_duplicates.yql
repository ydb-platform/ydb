--!syntax_pg
/* custom error:ORDER BY column reference 'y' is ambigous*/
select x + 1 as y, x - 1 as y from
    (select 1 x) a
order by y
-- order by y is ambigous
