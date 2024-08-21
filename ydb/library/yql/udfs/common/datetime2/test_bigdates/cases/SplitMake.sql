/* syntax version 1 */

select
    DateTime::MakeDate32(d32) as dd,
    DateTime::MakeDate32(dt64) as ddt,
    DateTime::MakeDate32(ts64) as dts,
    DateTime::MakeDatetime64(d32) as dtd,
    DateTime::MakeDatetime64(dt64) as dtdt,
    DateTime::MakeDatetime64(ts64) as dtts,
    DateTime::MakeTimestamp64(d32) as tsd,
    DateTime::MakeTimestamp64(dt64) as tsdt,
    DateTime::MakeTimestamp64(ts64) as tsts
from Input;

select
    DateTime::MakeDate32(d) as dd,
    DateTime::MakeDate32(dt) as ddt,
    DateTime::MakeDate32(ts) as dts,
    DateTime::MakeDatetime64(d) as dtd,
    DateTime::MakeDatetime64(dt) as dtdt,
    DateTime::MakeDatetime64(ts) as dtts,
    DateTime::MakeTimestamp64(d) as tsd,
    DateTime::MakeTimestamp64(dt) as tsdt,
    DateTime::MakeTimestamp64(ts) as tsts
from InputNarrow;
