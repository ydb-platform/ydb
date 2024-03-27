/* postgres can not */
use plato;

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
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, l.i64*i8, l.i64*i16, l.i64*i32, l.i64*r.i64
, 2, l.i64/i8, l.i64/i16, l.i64/i32, l.i64/r.i64
from BigDates as l cross join Signed as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, i64*ui8, i64*ui16, i64*ui32, i64*ui64
, 2, i64/ui8, i64/ui16, i64/ui32, i64/ui64
from BigDates as l cross join Unsigned as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;
