/* postgres can not */
use plato;

select l.row, r.row
, 1, l.d32 - r.d32, l.d32 - r.dt64, l.d32 - r.ts64
, 2, l.dt64 - r.d32, l.dt64 - r.dt64, l.dt64 - r.ts64
, 3, l.ts64 - r.d32, l.ts64 - r.dt64, l.ts64 - r.ts64
from BigDates as l cross join BigDates as r
order by l.row, r.row;

select row, -i64, +i64, abs(i64)
from BigDates
order by row;

select min(d32), min(dt64), min(ts64), min(i64)
, max(d32), max(dt64), max(ts64), max(i64)
from BigDates;

select l.row, r.row
, 1, l.d32 - r.i64, l.dt64 - r.i64, l.ts64 - r.i64, l.i64 - r.i64
, 2, l.d32 + r.i64, l.dt64 + r.i64, l.ts64 + r.i64, l.i64 + r.i64
from BigDates as l cross join BigDates as r
order by l.row, r.row;

select l.row, r.row
, 1, l.d32 - r.d, l.d32 - r.dt, l.d32 - r.ts
, 2, l.dt64 - r.d, l.dt64 - r.dt, l.dt64 - r.ts
, 3, l.ts64 - r.d, l.ts64 - r.dt, l.ts64 - r.ts
from BigDates as l cross join NarrowDates as r
order by l.row, r.row;

select l.row, r.row
, 1, l.d - r.d32, l.d - r.dt64, l.d - r.ts64, l.d - r.i64, l.d + r.i64
, 2, l.dt - r.d32, l.dt - r.dt64, l.dt - r.ts64, l.dt - r.i64, l.dt + r.i64
, 3, l.ts - r.d32, l.ts - r.dt64, l.ts - r.ts64, l.ts - r.i64, l.ts + r.i64
from NarrowDates as l cross join BigDates as r
order by l.row, r.row;

select l.row, r.row
, 1, l.d32 - r.i, l.dt64 - r.i, l.ts64 - r.i, l.i64 - r.i
, 2, l.d32 + r.i, l.dt64 + r.i, l.ts64 + r.i, l.i64 + r.i
from BigDates as l cross join NarrowInterval as r
order by l.row, r.row;

select i.row, s.row
, 1, i.i64*i8, i.i64*i16, i.i64*i32, i.i64*s.i64
, 2, i.i64/i8, i.i64/i16, i.i64/i32, i.i64/s.i64
from BigDates as i cross join Signed as s
order by i.row, s.row;

select i.row, u.row
, 1, i64*ui8, i64*ui16, i64*ui32, i64*ui64
, 2, i64/ui8, i64/ui16, i64/ui32, i64/ui64
from BigDates as i cross join Unsigned as u
order by i.row, u.row;
