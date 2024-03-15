/* postgres can not */
use plato;

select row
, 1, cast(d32 as date), cast(dt64 as date), cast(ts64 as date)
, 2, cast(d32 as datetime), cast(dt64 as datetime), cast(ts64 as datetime)
, 3, cast(d32 as timestamp), cast(dt64 as timestamp), cast(ts64 as timestamp)
, 4, cast(i64 as interval)
from BigDates;

-- TODO
