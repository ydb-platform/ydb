--!syntax_pg
select a.* from (
    plato."Input" a
    join
    plato."Input"
    using(key)) order by key