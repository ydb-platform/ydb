insert into plato.Output
select * from (select IF(key == "foo", CombineMembers(RemoveMembers(LAG(data) OVER w, ["key"]), ChooseMembers(data, ["key"])), data) from 
    (select TableRow() as data, key, value from plato.Input)
window w as (partition by key)) flatten columns;