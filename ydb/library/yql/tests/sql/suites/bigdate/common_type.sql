/* postgres can not */
use plato;

$date_val = unwrap(cast(1 as date));
$date32_val = unwrap(cast(-1 as date32));

$datetime_val = unwrap(cast(86400 as datetime));
$datetime64_val = unwrap(cast(-86400 as datetime64));

$timestamp_val = unwrap(cast(86400l*1000000 as timestamp));
$timestamp64_val = unwrap(cast(-86400l*1000000 as timestamp64));

select 1, [$date_val, $datetime_val]
, [$date_val, $timestamp_val]
, [$date_val, $date32_val]
, [$date_val, $datetime64_val]
, [$date_val, $timestamp64_val]
, 2, [$datetime_val, $date_val]
, [$datetime_val, $timestamp_val]
, [$datetime_val, $date32_val]
, [$datetime_val, $datetime64_val]
, [$datetime_val, $timestamp64_val]
, 3, [$timestamp_val, $date_val]
, [$timestamp_val, $datetime_val]
, [$timestamp_val, $date32_val]
, [$timestamp_val, $datetime64_val]
, [$timestamp_val, $timestamp64_val]
, 4, [$date32_val, $date_val]
, [$date32_val, $datetime_val]
, [$date32_val, $timestamp_val]
, [$date32_val, $datetime64_val]
, [$date32_val, $timestamp64_val]
, 5, [$datetime64_val, $date_val]
, [$datetime64_val, $datetime_val]
, [$datetime64_val, $timestamp_val]
, [$datetime64_val, $date32_val]
, [$datetime64_val, $timestamp64_val]
, 6, [$timestamp64_val, $date_val]
, [$timestamp64_val, $datetime_val]
, [$timestamp64_val, $timestamp_val]
, [$timestamp64_val, $date32_val]
, [$timestamp64_val, $datetime64_val]
, 7,  [$date_val, $datetime_val, $timestamp_val, $date32_val, $datetime64_val, $timestamp64_val];

select [unwrap(cast(1 as interval)), unwrap(cast(-1 as interval64))];

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
