--!syntax_pg
select * from (
    (select 1 as fooo, 1 as x
    union all
    select 1 as fooo, 2 as y) c
    join
    (select 1 as fooo) d
    using(fooo)),(
    (select 2 as foo) a
    join
    (select 2 as bar) b
    on(a.foo = b.bar)) order by x