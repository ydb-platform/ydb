/* postgres can not */
use plato;

select l.row, r.row
, 1, l.i64*i8, l.i64*i16, l.i64*i32, l.i64*r.i64
, 2, i8*l.i64, i16*l.i64, i32*l.i64, r.i64*l.i64
, 3, l.i64/i8, l.i64/i16, l.i64/i32, l.i64/r.i64
from BigDates as l cross join Signed as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, i64*ui8, i64*ui16, i64*ui32, i64*ui64
, 2, ui8*i64, ui16*i64, ui32*i64, ui64*i64
, 3, i64/ui8, i64/ui16, i64/ui32, i64/ui64
from BigDates as l cross join Unsigned as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, l.i*i8, l.i*i16, l.i*i32, l.i*r.i64
, 2, i8*l.i, i16*l.i, i32*l.i, r.i64*l.i
, 3, l.i/i8, l.i/i16, l.i/i32, l.i/r.i64
from NarrowInterval as l cross join Signed as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, i*ui8, i*ui16, i*ui32, i*ui64
, 2, ui8*i, ui16*i, ui32*i, ui64*i
, 3, i/ui8, i/ui16, i/ui32, i/ui64
from NarrowInterval as l cross join Unsigned as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

