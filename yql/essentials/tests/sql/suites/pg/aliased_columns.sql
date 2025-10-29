--!syntax_pg
select a, b, c from (select 1 as a, 2 as b, 3 as c) as "A.B" order by b
