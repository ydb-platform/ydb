--!syntax_pg
select * from 
    ((select 1 as foo, 1 as x) aa 
    join
    (select 1 as foo, 2 as bar) bb
    using (foo)
    join
    (select 2 as zoo, 1 as x) cc
    using (x)
    )