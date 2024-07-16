/* syntax version 1 */
select
    DateTime::MakeDate32(DateTime::Split(d32)) as d32,
    DateTime::MakeDatetime64(DateTime::Split(dt64)) as dt64,
    DateTime::MakeTimestamp64(DateTime::Split(ts64)) as ts64
from Input;

select
    DateTime::MakeTzDate32(DateTime::Split(cast(tzd32 as TzDate32))) as tzd32,
    DateTime::MakeTzDatetime64(DateTime::Split(cast(tzdt64 as TzDatetime64))) as tzdt64,
    DateTime::MakeTzTimestamp64(DateTime::Split(cast(tzts64 as TzTimestamp64))) as tzts64
from InputTz;
