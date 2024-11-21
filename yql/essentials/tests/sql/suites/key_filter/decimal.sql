/* postgres can not */
/* syntax version 1 */
use plato;

$asIs = Python::asIs(Callable<(String)->String>,
@@
def asIs(arg):
    return arg
@@
);

insert into @decimal
select cast(value as Decimal(15,10)) as value from Input order by value;

commit;

select * from (
    select * from @decimal where value < Decimal("4.1",15,10) and value > Decimal("10.5",15,10) -- empty
    union all
    select * from @decimal where value > Decimal("inf",15,10) -- empty
    union all
    select * from @decimal where value < Decimal("-inf",15,10) -- empty
    union all
    select * from @decimal where value = Decimal("nan",15,10) -- empty
    union all
    select * from @decimal where value = Decimal("inf",15,10)
    union all
    select * from @decimal where value = Decimal("-inf",15,10)
    union all
    select * from @decimal where value > Decimal("3.3",15,10) or value >= Decimal("3.30001",15,10)
)
order by value;

-- Don't union all to calc nodes separatelly
select * from @decimal where value = cast("6.6" as Decimal(15,10)); -- Safe key filter calc
select * from @decimal where value = cast($asIs("3.3") as Decimal(15,10)); -- Unsafe key filter calc
select * from @decimal where value = cast($asIs("bad") as Decimal(15,10)); -- Unsafe key filter calc
