--!syntax_pg
select c.fooo from (
    (select 1 as fooo, 1 as x
    union all
    select 1 as fooo, 2 as y) c
    join
    (select 1 as fooo, 3 as xy) d
    using(fooo))