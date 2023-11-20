/* postgres can not */
$list = select lst, row_number() over (order by lst) as rn from (
    select * from (
        select ListFromRange(1us, 333us) as lst
    ) FLATTEN LIST by lst
);

$usr = select value, CAST(key AS Uint16) + 3us AS age, CAST(key AS Uint16) + 7us as age1
from plato.Input;

select
    u.*,
    l1.rn as rn1,
    l2.rn as rn2
from  $usr as u
join $list as l1 on u.age == l1.lst
join $list as l2 on u.age1 == l2.lst
order by value;
