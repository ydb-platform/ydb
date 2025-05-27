/* postgres can not */
use plato;

select l.row, r.row
, 1, l.d32 - r.d32, l.d32 - r.dt64, l.d32 - r.ts64
, 2, l.dt64 - r.d32, l.dt64 - r.dt64, l.dt64 - r.ts64
, 3, l.ts64 - r.d32, l.ts64 - r.dt64, l.ts64 - r.ts64
from BigDates as l cross join BigDates as r
where abs(l.row) <= 7 and abs(r.row) <= 7
order by l.row, r.row;
