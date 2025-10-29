--!syntax_pg
select c.fooo from (
    (select 1 as fooo) c
    full join
    (select 2 as fooo) d
    using(fooo)) order by fooo