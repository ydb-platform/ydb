--!syntax_pg
select * from 
    ((select 1 as foo, 1 as bar, 1 as zoo) aa 
    join
    (select 1 as foo, 1 as bar, 1 as zoo) bb
    using (foo)
    join
    (select 1 as bar, 1 as zoo) cc
    using (zoo)
    )