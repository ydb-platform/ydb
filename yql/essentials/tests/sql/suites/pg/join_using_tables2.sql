--!syntax_pg
select a.* from (
    plato."Input"
    join
    plato."Input" a
    using(key)) order by key