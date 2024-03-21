--!syntax_pg
select a.fooo as x, b.fooo as y, c.fooo as z, fooo from (
    (select 1 as fooo) a
    full join
    (select 2 as fooo) b
    using (fooo)
    full join
    (select 3 as fooo) c
    using(fooo)) order by fooo