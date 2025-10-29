--!syntax_pg
select key from (
    plato."Input" a
    full join
    plato."Input5"
    using(key)) order by key