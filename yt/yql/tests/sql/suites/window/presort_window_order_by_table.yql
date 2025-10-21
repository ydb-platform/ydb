/* postgres can not */
use plato;

$list = AsList(
        AsList(3,1),
        AsList(1,1),
        AsList(1),
    );

insert into @foo
select x from (select $list as x) 
flatten by x;
commit;
select x,row_number() over w as r from @foo
window w as (order by x asc);

