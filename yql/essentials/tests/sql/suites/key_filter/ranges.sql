/* postgres can not */
select * from (
    select * from plato.Input where key<="037" and key>="037"
    union all
    select * from plato.Input where key>="037" and key<="037"
    union all
    select * from plato.Input where key between "037" and "037"
)
order by key,subkey;
