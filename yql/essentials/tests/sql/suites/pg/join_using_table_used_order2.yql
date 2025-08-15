--!syntax_pg
/* custom error:common column name "zoo" appears more than once in left table*/
select * from
    ((select 1 as foo, 1 as bar, 1 as zoo) aa
    join
    (select 1 as foo, 1 as bar, 1 as zoo) bb
    using (foo)
    join
    (select 1 as bar, 1 as zoo) cc
    using (zoo)
    )
