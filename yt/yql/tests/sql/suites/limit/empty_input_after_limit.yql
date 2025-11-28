/* postgres can not */
$in = (
    select * from plato.Input where key = "150"
    union all
    select * from plato.Input where key = "075"
);

select * from $in order by key limit 100 offset 90;
