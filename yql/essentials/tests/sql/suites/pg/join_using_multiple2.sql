--!syntax_pg
select * from (
    (plato."Input"
    join
    plato."Input2"
    using (id))
    join
    plato."Input3"
    using (id)) order by id