/* postgres can not */
use plato;

select l.row, r.row
, 1, l.d32 - r.d, l.d32 - r.dt, l.d32 - r.ts
, 2, l.dt64 - r.d, l.dt64 - r.dt, l.dt64 - r.ts
, 3, l.ts64 - r.d, l.ts64 - r.dt, l.ts64 - r.ts
from BigDates as l cross join NarrowDates as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, l.d - r.d32, l.d - r.dt64, l.d - r.ts64, l.d - r.i64, l.d + r.i64
, 2, l.dt - r.d32, l.dt - r.dt64, l.dt - r.ts64, l.dt - r.i64, l.dt + r.i64
, 3, l.ts - r.d32, l.ts - r.dt64, l.ts - r.ts64, l.ts - r.i64, l.ts + r.i64
from NarrowDates as l cross join BigDates as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;

select l.row, r.row
, 1, l.d32 - r.i, l.dt64 - r.i, l.ts64 - r.i, l.i64 - r.i
, 2, l.d32 + r.i, l.dt64 + r.i, l.ts64 + r.i, l.i64 + r.i
from BigDates as l cross join NarrowInterval as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;
