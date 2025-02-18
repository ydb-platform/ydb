/* postgres can not */
use plato;

select row
, 1, cast(i8 as date32), cast(i8 as datetime64), cast(i8 as timestamp64), cast(i8 as interval64)
, 2, cast(i16 as date32), cast(i16 as datetime64), cast(i16 as timestamp64), cast(i16 as interval64)
, 3, cast(i32 as date32), cast(i32 as datetime64), cast(i32 as timestamp64), cast(i32 as interval64)
, 4, cast(i64 as date32), cast(i64 as datetime64), cast(i64 as timestamp64), cast(i64 as interval64)
from Signed
order by row;

select row
, 1, cast(d32 as int8), cast(d32 as int16), cast(d32 as int32), cast(d32 as int64)
, 2, cast(dt64 as int8), cast(dt64 as int16), cast(dt64 as int32), cast(dt64 as int64)
, 3, cast(ts64 as int8), cast(ts64 as int16), cast(ts64 as int32), cast(ts64 as int64)
, 4, cast(i64 as int8), cast(i64 as int16), cast(i64 as int32), cast(i64 as int64)
from BigDates
order by row;

select row
, 1, cast(d32 as uint8), cast(d32 as uint16), cast(d32 as uint32), cast(d32 as uint64)
, 2, cast(dt64 as uint8), cast(dt64 as uint16), cast(dt64 as uint32), cast(dt64 as uint64)
, 3, cast(ts64 as uint8), cast(ts64 as uint16), cast(ts64 as uint32), cast(ts64 as uint64)
, 4, cast(i64 as uint8), cast(i64 as uint16), cast(i64 as uint32), cast(i64 as uint64)
from BigDates
order by row;

select row
, 1, cast(ui8 as date32), cast(ui8 as datetime64), cast(ui8 as timestamp64), cast(ui8 as interval64)
, 2, cast(ui16 as date32), cast(ui16 as datetime64), cast(ui16 as timestamp64), cast(ui16 as interval64)
, 3, cast(ui32 as date32), cast(ui32 as datetime64), cast(ui32 as timestamp64), cast(ui32 as interval64)
, 4, cast(ui64 as date32), cast(ui64 as datetime64), cast(ui64 as timestamp64), cast(ui64 as interval64)
from Unsigned
order by row;
