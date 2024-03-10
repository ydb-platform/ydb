--!syntax_pg
select key from (
    plato."Input" a
    full join
    plato."Input3"
    using(key))