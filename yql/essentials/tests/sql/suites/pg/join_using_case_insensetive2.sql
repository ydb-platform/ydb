--!syntax_pg
select c.FOO as a, d.* from (
    (select 1 as FOO) c
    join
    (select 1 as foo) d
    using(Foo))