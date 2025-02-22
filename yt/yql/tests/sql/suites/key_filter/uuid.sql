/* postgres can not */
/* syntax version 1 */
use plato;

$asIs = Python::asIs(Callable<(String)->String>,
@@
def asIs(arg):
    return arg
@@
);

insert into @uuid
select cast(value as Uuid) as value from Input order by value;

commit;

select * from (
    select * from @uuid where value < Uuid("00000000-0000-0000-0000-100000000000") and value > Uuid("00000000-0000-0000-0000-400000000000") -- empty
    union all
    select * from @uuid where value > Uuid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF") -- empty
    union all
    select * from @uuid where value < Uuid("00000000-0000-0000-0000-000000000000") -- empty
    union all
    select * from @uuid where value > Uuid("00000000-0000-0000-0000-100000000000") or value >= Uuid("00000000-0000-0000-0000-200000000000") --(00000000-0000-0000-0000-100000000000,)
    union all
    select * from @uuid where value >= Uuid("00000000-0000-0000-0000-100000000000") or value > Uuid("00000000-0000-0000-0000-200000000000") --[00000000-0000-0000-0000-100000000000,)
    union all
    select * from @uuid where value = Uuid("00000000-0000-0000-0000-100000000000") or value < Uuid("00000000-0000-0000-0000-200000000000") --(,00000000-0000-0000-0000-200000000000)
    union all
    select * from @uuid where value < Uuid("00000000-0000-0000-0000-100000000000") or value <= Uuid("00000000-0000-0000-0000-200000000000") --(,00000000-0000-0000-0000-200000000000]
    union all
    select * from @uuid where value < Uuid("00000000-0000-0000-0000-100000000000") or value <= Uuid("00000000-0000-0000-0000-200000000000") --(,00000000-0000-0000-0000-200000000000]
    union all
    select * from @uuid where value > Uuid("00000000-0000-0000-0000-100000000000") and value <= Uuid("00000000-0000-0000-0000-400000000000") --(00000000-0000-0000-0000-100000000000,00000000-0000-0000-0000-400000000000]
)
order by value;

-- Don't union all to calc nodes separatelly
select * from @uuid where value = cast("00000000-0000-0000-0000-100000000000" as Uuid); -- Safe key filter calc
select * from @uuid where value = cast($asIs("00000000-0000-0000-0000-200000000000") as Uuid); -- Unsafe key filter calc
select * from @uuid where value = cast($asIs("bad") as Uuid); -- Unsafe key filter calc
