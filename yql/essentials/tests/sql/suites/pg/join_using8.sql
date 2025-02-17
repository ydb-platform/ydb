--!syntax_pg
select c.fooo as x, d.fooo as y from (
    (select 1 as fooo, 1 as x) c
    full join
    (select 2 as fooo, 3 as xy) d
    using(fooo)) order by x, y