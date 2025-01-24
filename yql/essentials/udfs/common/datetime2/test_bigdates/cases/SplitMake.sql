/* syntax version 1 */

select
    DateTime::MakeDate32(d32)       as dd,   DateTime::MakeDate32(DateTime::Split(d32))       as sdd,  
    DateTime::MakeDate32(dt64)      as ddt,  DateTime::MakeDate32(DateTime::Split(dt64))      as sddt, 
    DateTime::MakeDate32(ts64)      as dts,  DateTime::MakeDate32(DateTime::Split(ts64))      as sdts, 
    DateTime::MakeDatetime64(d32)   as dtd,  DateTime::MakeDatetime64(DateTime::Split(d32))   as sdtd, 
    DateTime::MakeDatetime64(dt64)  as dtdt, DateTime::MakeDatetime64(DateTime::Split(dt64))  as sdtdt,
    DateTime::MakeDatetime64(ts64)  as dtts, DateTime::MakeDatetime64(DateTime::Split(ts64))  as sdtts,
    DateTime::MakeTimestamp64(d32)  as tsd,  DateTime::MakeTimestamp64(DateTime::Split(d32))  as stsd, 
    DateTime::MakeTimestamp64(dt64) as tsdt, DateTime::MakeTimestamp64(DateTime::Split(dt64)) as stsdt,
    DateTime::MakeTimestamp64(ts64) as tsts, DateTime::MakeTimestamp64(DateTime::Split(ts64)) as ststs 
from Input
order by d32;

select
    DateTime::MakeDate32(d)       as dd,   DateTime::MakeDate32(DateTime::Split(d))       as sdd,
    DateTime::MakeDate32(dt)      as ddt,  DateTime::MakeDate32(DateTime::Split(dt))      as sddt,
    DateTime::MakeDate32(ts)      as dts,  DateTime::MakeDate32(DateTime::Split(ts))      as sdts,
    DateTime::MakeDatetime64(d)   as dtd,  DateTime::MakeDatetime64(DateTime::Split(d))   as sdtd,
    DateTime::MakeDatetime64(dt)  as dtdt, DateTime::MakeDatetime64(DateTime::Split(dt))  as sdtdt,
    DateTime::MakeDatetime64(ts)  as dtts, DateTime::MakeDatetime64(DateTime::Split(ts))  as sdtts,
    DateTime::MakeTimestamp64(d)  as tsd,  DateTime::MakeTimestamp64(DateTime::Split(d))  as stsd,
    DateTime::MakeTimestamp64(dt) as tsdt, DateTime::MakeTimestamp64(DateTime::Split(dt)) as stsdt,
    DateTime::MakeTimestamp64(ts) as tsts, DateTime::MakeTimestamp64(DateTime::Split(ts)) as ststs
from InputNarrow
order by d;
