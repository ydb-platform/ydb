/* syntax version 1 */
select
    DateTime::MakeDate32(d16) as d16_32, -- TODO accept narrow TMStorage
    DateTime::MakeDate32(d32) as d32,
    DateTime::MakeDatetime64(dt64) as dt64,
    DateTime::MakeTimestamp64(ts64) as ts64
from Input;

$tzDates = select rn, tz
    , cast(d32 || ',' || tz as TzDate32) as tzd32
    , cast(dt64 || ',' || tz as TzDatetime64) as tzdt64
    , cast(ts64 || ',' || tz as TzTimestamp64) as tzts64
from InputTz cross join Tz;

select rn, tz
, DateTime::MakeTzDate32(tzd32) as tzd32
, DateTime::MakeTzDatetime64(tzdt64) as tzdt64
, DateTime::MakeTzTimestamp64(tzts64) as tzts64
from $tzDates
order by rn, tz
