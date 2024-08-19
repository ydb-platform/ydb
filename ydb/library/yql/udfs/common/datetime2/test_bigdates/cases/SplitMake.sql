/* syntax version 1 */
select
    DateTime::MakeDate32(DateTime::Split(d16)) as d16_32, -- TODO accept narrow TMStorage
    DateTime::MakeDate32(DateTime::Split(d32)) as d32,
    DateTime::MakeDatetime64(DateTime::Split(dt64)) as dt64,
    DateTime::MakeTimestamp64(DateTime::Split(ts64)) as ts64
from Input;

$tzDates = select rn, tz
    , cast(d32 || ',' || tz as TzDate32) as tzd32
    , cast(dt64 || ',' || tz as TzDatetime64) as tzdt64
    , cast(ts64 || ',' || tz as TzTimestamp64) as tzts64
from InputTz cross join Tz;

select rn, tz
, DateTime::MakeTzDate32(DateTime::Split(tzd32)) as tzd32
, DateTime::MakeTzDatetime64(DateTime::Split(tzdt64)) as tzdt64
, DateTime::MakeTzTimestamp64(DateTime::Split(tzts64)) as tzts64
from $tzDates
order by rn, tz
