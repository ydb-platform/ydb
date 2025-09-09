use plato;

pragma yt.MapJoinLimit="1m";

$t1 = AsList(
    AsStruct(Just(1) AS Key),
    AsStruct(Just(2) AS Key),
    AsStruct(Just(3) AS Key));

$t2 = AsList(
    AsStruct(Just(Just(2)) AS Key),
    AsStruct(Just(Just(3)) AS Key),
    AsStruct(Just(Just(4)) AS Key),
    AsStruct(Just(Just(5)) AS Key),
    AsStruct(Just(Just(6)) AS Key));

insert into @t1 select * from as_table($t1);
insert into @t2 select * from as_table($t2);

commit;

select *
from @t1 as a join @t2 as b using(Key);
