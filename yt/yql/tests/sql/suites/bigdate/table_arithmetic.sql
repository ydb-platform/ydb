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
