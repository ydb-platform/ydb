/* postgres can not */
use plato;

$t1 = AsList(
    AsStruct(75 as key, 1 as subkey),
    AsStruct(800 as key, 2 as subkey));

insert into @t1
select * from AS_TABLE($t1);

commit;

$tuples = (select AsTuple(key, subkey) from @t1);

select * from Input
where AsTuple(cast(key as uint64), cast(subkey as uint64)) in $tuples
