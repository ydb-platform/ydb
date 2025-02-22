/* postgres can not */
/* multirun can not */
/* syntax version 1 */
use plato;

$t = AsList(
    AsStruct(1 as key, 101 as value)
);

insert into Output
select * from as_table($t) order by key;
