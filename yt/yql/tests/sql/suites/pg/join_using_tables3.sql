--!syntax_pg
select * from (
    plato."Input" a
    full join
    plato."Input"
    using(key,subkey,value)) order by key