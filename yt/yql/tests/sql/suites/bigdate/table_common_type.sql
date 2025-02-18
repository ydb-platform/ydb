/* postgres can not */
use plato;

select bd.row as row
, 1, [d, dt]
, [d, ts]
, [d, d32]
, [d, dt64]
, [d, ts64]
, 2, [dt, ts]
, [dt, d32]
, [dt, dt64]
, [dt, ts64]
, 3, [ts, d32]
, [ts, dt64]
, [ts, ts64]
, 4, [d32, dt64]
, [d32, ts64]
, 5, [dt64, ts64]
from BigDates as bd
join NarrowDates using (row)
order by row;

select bd.row as row, [i, i64]
from BigDates as bd
join NarrowInterval using (row)
order by row;
