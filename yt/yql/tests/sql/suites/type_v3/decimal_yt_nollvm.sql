/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";

pragma config.flags("LLVM", "OFF");

insert into @a
select * from (
    select Decimal("3.14",3,2) as d3, Decimal("2.9999999999",12,10) as d12, Decimal("2.12345678900876543",35,10) as d35
    union all
    select Decimal("inf",3,2) as d3, Decimal("inf",12,10) as d12, Decimal("inf",35,10) as d35
    union all
    select Decimal("-inf",3,2) as d3, Decimal("-inf",12,10) as d12, Decimal("-inf",35,10) as d35
    union all
    select Decimal("nan",3,2) as d3, Decimal("nan",12,10) as d12, Decimal("nan",35,10) as d35
);

commit;

select * from @a where d3 != Decimal("5.3",3,2);

select * from Input;
