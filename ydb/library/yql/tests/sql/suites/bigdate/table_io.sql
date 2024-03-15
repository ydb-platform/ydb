/* postgres can not */
use plato;

select * from BigDates;

insert into @Output with truncate
select * from BigDates
where row > -100;

commit;

select * from @Output;

select row, cast(d32 as string), cast(dt64 as string), cast(ts64 as string), cast(i64 as string)
from BigDates;
