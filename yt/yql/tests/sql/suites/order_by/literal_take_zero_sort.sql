/* postgres can not */
/* multirun can not */
/* syntax version 1 */
use plato;

$t = AsList(
    AsStruct(1 as key, 101 as value),
    AsStruct(2 as key, 34 as value),
    AsStruct(4 as key, 22 as value),
    AsStruct(6 as key, 256 as value),
    AsStruct(7 as key, 111 as value)
);

insert into Output
select * from as_table($t) order by key limit 0;
