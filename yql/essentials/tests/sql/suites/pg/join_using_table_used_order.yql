--!syntax_pg
select * from 
    ((select 1 as foo) aa 
    join
    (select 1 as foo, 2 as bar) bb
    using (foo)
    join
    (select 2 as bar, 1 as x) cc
    using (bar)
    join
    (select 1 as x) dd
    using (x)
    )