/* postgres can not */
use plato;

select row, cast(i as interval64) from NarrowInterval;

select row
, 1, cast(d as date), cast(dt as date), cast(ts as date)
, 2, cast(d as datetime), cast(dt as datetime), cast(ts as datetime)
, 3, cast(d as timestamp), cast(dt as timestamp), cast(ts as timestamp)
from NarrowDates;

select row
, 1, cast(d as date32), cast(dt as date32), cast(ts as date32)
, 2, cast(d as datetime64), cast(dt as datetime64), cast(ts as datetime64)
, 3, cast(d as timestamp64), cast(dt as timestamp64), cast(ts as timestamp64)
from NarrowDates;

select row
, 1, cast(d32 as date), cast(dt64 as date), cast(ts64 as date)
, 2, cast(d32 as datetime), cast(dt64 as datetime), cast(ts64 as datetime)
, 3, cast(d32 as timestamp), cast(dt64 as timestamp), cast(ts64 as timestamp)
, 4, cast(i64 as interval)
from BigDates;

select row
, 1, cast(d32 as date32), cast(dt64 as date32), cast(ts64 as date32)
, 2, cast(d32 as datetime64), cast(dt64 as datetime64), cast(ts64 as datetime64)
, 3, cast(d32 as timestamp64), cast(dt64 as timestamp64), cast(ts64 as timestamp64)
, 4, cast(i64 as interval64)
from BigDates;

