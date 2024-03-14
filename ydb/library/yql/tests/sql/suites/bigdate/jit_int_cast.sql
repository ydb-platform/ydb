/* postgres can not */
use plato;

select row
, 1, cast(i8 as date32), cast(i8 as datetime64), cast(i8 as timestamp64), cast(i8 as interval64)
, 2, cast(i16 as date32), cast(i16 as datetime64), cast(i16 as timestamp64), cast(i16 as interval64)
, 3, cast(i32 as date32), cast(i32 as datetime64), cast(i32 as timestamp64), cast(i32 as interval64)
, 4, cast(i16 as date32), cast(i16 as datetime64), cast(i16 as timestamp64), cast(i16 as interval64)
, 5, cast(ui8 as date32), cast(ui8 as datetime64), cast(ui8 as timestamp64), cast(ui8 as interval64)
, 6, cast(ui16 as date32), cast(ui16 as datetime64), cast(ui16 as timestamp64), cast(ui16 as interval64)
, 7, cast(ui32 as date32), cast(ui32 as datetime64), cast(ui32 as timestamp64), cast(ui32 as interval64)
, 8, cast(ui16 as date32), cast(ui16 as datetime64), cast(ui16 as timestamp64), cast(ui16 as interval64)
from Integers;
