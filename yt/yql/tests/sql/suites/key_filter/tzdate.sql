/* postgres can not */
/* syntax version 1 */
use plato;

$asIs = Python::asIs(Callable<(String)->String>,
@@
def asIs(arg):
    return arg
@@
);

insert into @tzdate
select cast(value as TzDate) as value from Input order by value;

commit;

select * from (
    select * from @tzdate where value < TzDate("1999-01-01,Europe/Moscow") and value > TzDate("2011-01-01,Europe/Moscow") -- empty
    union all
    select * from @tzdate where value > TzDate("2105-12-30,posixrules") -- empty
    union all
    select * from @tzdate where value < TzDate("1970-01-01,GMT") -- empty
    union all
    select * from @tzdate where value = TzDate("2018-02-01,GMT")
    union all
    select * from @tzdate where value > TzDate("1999-01-01,GMT") or value >= TzDate("1999-01-01,Europe/Moscow")
    union all
    select * from @tzdate where value >= TzDate("2018-02-01,Europe/Moscow") and value <= TzDate("2105-12-30,America/Los_Angeles") -- Should include 2018-02-01,GMT and 2105-12-31,posixrules
)
order by value;

-- Don't union all to calc nodes separatelly
select * from @tzdate where value = cast("1999-01-01,Europe/Moscow" as TzDate); -- Safe key filter calc
select * from @tzdate where value = cast($asIs("2105-12-30,America/Los_Angeles") as TzDate); -- Unsafe key filter calc
select * from @tzdate where value = cast($asIs("bad") as TzDate); -- Unsafe key filter calc
