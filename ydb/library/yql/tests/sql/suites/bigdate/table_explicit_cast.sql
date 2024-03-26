/* postgres can not */
use plato;

select row, cast(i as interval64) from NarrowInterval order by row;

select row
, 1, cast(d as date), cast(d as datetime), cast(d as timestamp)
, 2, cast(dt as date), cast(dt as datetime), cast(dt as timestamp)
, 3, cast(ts as date), cast(ts as datetime), cast(ts as timestamp)
from NarrowDates order by row;

select row
, 1, cast(d as date32), cast(d as datetime64), cast(d as timestamp64)
, 2, cast(dt as date32), cast(dt as datetime64), cast(dt as timestamp64)
, 3, cast(ts as date32), cast(ts as datetime64), cast(ts as timestamp64)
from NarrowDates order by row;

select row
, 1, cast(d32 as date), cast(d32 as datetime), cast(d32 as timestamp)
, 2, cast(dt64 as date), cast(dt64 as datetime), cast(dt64 as timestamp)
, 3, cast(ts64 as date), cast(ts64 as datetime), cast(ts64 as timestamp)
, 4, cast(i64 as interval)
from BigDates order by row;

select row
, 1, cast(d32 as datetime64), cast(d32 as timestamp64)
, 2, cast(dt64 as date32), cast(dt64 as timestamp64)
, 3, cast(ts64 as date32), cast(ts64 as datetime64)
from BigDates order by row;
