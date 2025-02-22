insert into plato.Output WITH TRUNCATE
select * from (select lag(data) over w from
    (select TableRow() as data, key from plato.Input)
window w as (partition by key)) flatten columns;
