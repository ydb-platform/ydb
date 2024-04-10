$date32_min = unwrap(cast(-53375809 as date32));
$date32_max = unwrap(cast(53375807 as date32));
$datetime64_min = unwrap(cast(-4611669897600 as datetime64));
$datetime64_max = unwrap(cast(4611669811199 as datetime64));
$timestamp64_min = unwrap(cast(-4611669897600000000 as timestamp64));
$timestamp64_max = unwrap(cast(4611669811199999999 as timestamp64));
$interval64_min = unwrap(cast(-9223339708799999999 as interval64));
$interval64_max = unwrap(cast(9223339708799999999 as interval64));
$interval64_plus1 = unwrap(cast(1 as interval64));
$interval64_minus1 = unwrap(cast(-1 as interval64));

$date_max_value = 49673l;
$date_max = unwrap(cast($date_max_value - 1 as date));
$datetime_max = unwrap(cast($date_max_value*86400 - 1 as datetime));
$timestamp_max = unwrap(cast($date_max_value*86400*1000000 - 1 as timestamp));
$interval_min = unwrap(cast(-$date_max_value*86400*1000000 + 1 as interval));
$interval_max = unwrap(cast($date_max_value*86400*1000000 - 1 as interval));
$interval_plus1 = unwrap(cast(1 as interval));
$interval_minus1 = unwrap(cast(-1 as interval));

select 1, $date32_min - $date32_max, $date32_max - $date32_min
, $date32_min - $datetime64_max, $date32_max - $datetime64_min
, $date32_min - $timestamp64_max, $date32_max - $timestamp64_min
, 2, $date32_min - $date_max
, $date32_min - $datetime_max
, $date32_min - $timestamp_max
, 3, $date32_min - $interval64_minus1, $date32_max - $interval64_minus1
, $date32_min - $interval64_plus1, $date32_max - $interval64_plus1
, $date32_min + $interval64_minus1, $date32_max + $interval64_minus1
, $date32_min + $interval64_plus1, $date32_max + $interval64_plus1
, 4, $date32_min - $interval_minus1, $date32_max - $interval_minus1
, $date32_min - $interval_plus1, $date32_max - $interval_plus1
, $date32_min + $interval_minus1, $date32_max + $interval_minus1
, $date32_min + $interval_plus1, $date32_max + $interval_plus1;

select 1, $datetime64_min - $date32_max, $datetime64_max - $date32_min
, $datetime64_min - $datetime64_max, $datetime64_max - $datetime64_min
, $datetime64_min - $timestamp64_max, $datetime64_max - $timestamp64_min
, 2, $datetime64_min - $date_max
, $datetime64_min - $datetime_max
, $datetime64_min - $timestamp_max
, 3, $datetime64_min - $interval64_minus1, $datetime64_max - $interval64_minus1
, $datetime64_min - $interval64_plus1, $datetime64_max - $interval64_plus1
, $datetime64_min + $interval64_minus1, $datetime64_max + $interval64_minus1
, $datetime64_min + $interval64_plus1, $datetime64_max + $interval64_plus1
, 4, $datetime64_min - $interval_minus1, $datetime64_max - $interval_minus1
, $datetime64_min - $interval_plus1, $datetime64_max - $interval_plus1
, $datetime64_min + $interval_minus1, $datetime64_max + $interval_minus1
, $datetime64_min + $interval_plus1, $datetime64_max + $interval_plus1;

select 1, $timestamp64_min - $date32_max, $timestamp64_max - $date32_min
, $timestamp64_min - $datetime64_max, $timestamp64_max - $datetime64_min
, $timestamp64_min - $timestamp64_max, $timestamp64_max - $timestamp64_min
, 2, $timestamp64_min - $date_max
, $timestamp64_min - $datetime_max
, $timestamp64_min - $timestamp_max
, 3, $timestamp64_min - $interval64_minus1, $timestamp64_max - $interval64_minus1
, $timestamp64_min - $interval64_plus1, $timestamp64_max - $interval64_plus1
, $timestamp64_min + $interval64_minus1, $timestamp64_max + $interval64_minus1
, $timestamp64_min + $interval64_plus1, $timestamp64_max + $interval64_plus1
, 4, $timestamp64_min - $interval_minus1, $timestamp64_max - $interval_minus1
, $timestamp64_min - $interval_plus1, $timestamp64_max - $interval_plus1
, $timestamp64_min + $interval_minus1, $timestamp64_max + $interval_minus1
, $timestamp64_min + $interval_plus1, $timestamp64_max + $interval_plus1;

select 1, $date_max - $date32_min, $date_max - $datetime64_min, $date_max - $timestamp64_min
, $date_max - $date32_max, $date_max - $datetime64_max, $date_max - $timestamp64_max
, $date_max - $interval64_minus1, $date_max + $interval64_minus1
, $date_max - $interval64_plus1, $date_max + $interval64_plus1
, 2, $datetime_max - $date32_min, $datetime_max - $datetime64_min, $datetime_max - $timestamp64_min
, $datetime_max - $date32_max, $datetime_max - $datetime64_max, $datetime_max - $timestamp64_max
, $datetime_max - $interval64_minus1, $datetime_max + $interval64_minus1
, $datetime_max - $interval64_plus1, $datetime_max + $interval64_plus1
, 3, $timestamp_max - $date32_min, $timestamp_max - $datetime64_min, $timestamp_max - $timestamp64_min
, $timestamp_max - $date32_max, $timestamp_max - $datetime64_max, $timestamp_max - $timestamp64_max
, $timestamp_max - $interval64_minus1, $timestamp_max + $interval64_minus1
, $timestamp_max - $interval64_plus1, $timestamp_max + $interval64_plus1;

select 1, $interval_min - $interval64_min, $interval_min + $interval64_min
, $interval_min - $interval64_max, $interval_min + $interval64_max
, $interval_max - $interval64_max, $interval_max + $interval64_max
, $interval_max - $interval64_min, $interval_max + $interval64_min
, 2, $interval64_max - $interval64_min, $interval64_min - $interval64_max
, $interval64_max + $interval64_min, $interval64_max + $interval64_max
, $interval64_min - $interval64_min, $interval64_max - $interval64_max;

select $interval64_minus1*2, $interval64_plus1*2
, $interval64_min/2, $interval64_max/2
, -$interval64_min, -$interval64_max
, abs($interval64_min), abs($interval64_max);
