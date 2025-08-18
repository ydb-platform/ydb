$date_val = unwrap(cast(1 as date));
$date32_val = unwrap(cast(-1 as date32));

$datetime_val = unwrap(cast(86400 as datetime));
$datetime64_val = unwrap(cast(-86400 as datetime64));

$timestamp_val = unwrap(cast(86400l*1000000 as timestamp));
$timestamp64_val = unwrap(cast(-86400l*1000000 as timestamp64));

$interval_val = unwrap(cast(1 as interval));
$interval64_val = unwrap(cast(-1 as interval64));

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

$datetime_values = [$date_val, $date32_val, $datetime_val, $datetime64_val, $timestamp_val, $timestamp64_val];
$interval_values = [$interval_val, $interval64_val];
select ListSort(DictKeys(ToSet($datetime_values))), ListSort(DictKeys(ToSet($interval_values)));
