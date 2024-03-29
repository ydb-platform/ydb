--!syntax_pg
select * from (
    (select 1 as FOO) c
    join
    (select 1 as foo) d
    using(Foo))