insert into plato.Output
select * from (select IF(key == "foo", LAG(data) over w, data) from
    (select TableRow() as data, key, value from plato.Input)
window w as (partition by key)) flatten columns;