$date32_min = unwrap(CAST(-53375809 AS date32));
$date32_max = unwrap(CAST(53375807 AS date32));
$datetime64_min = unwrap(CAST(-4611669897600 AS datetime64));
$datetime64_max = unwrap(CAST(4611669811199 AS datetime64));
$timestamp64_min = unwrap(CAST(-4611669897600000000 AS timestamp64));
$timestamp64_max = unwrap(CAST(4611669811199999999 AS timestamp64));
$interval64_min = unwrap(CAST(-9223339708799999999 AS interval64));
$interval64_max = unwrap(CAST(9223339708799999999 AS interval64));
$interval64_plus1 = unwrap(CAST(1 AS interval64));
$interval64_minus1 = unwrap(CAST(-1 AS interval64));
$interval64_zero = unwrap(CAST(0 AS interval64));
$date_max_value = 49673l;
$date_max = unwrap(CAST($date_max_value - 1 AS date));
$datetime_max = unwrap(CAST($date_max_value * 86400 - 1 AS datetime));
$timestamp_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS timestamp));
$interval_min = unwrap(CAST(-$date_max_value * 86400 * 1000000 + 1 AS interval));
$interval_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS interval));
$interval_plus1 = unwrap(CAST(1 AS interval));
$interval_minus1 = unwrap(CAST(-1 AS interval));
$i64_max = 9223372036854775807l;
$ui64_max = 18446744073709551615ul;

SELECT
    1,
    $date32_min - $date32_max,
    $date32_max - $date32_min,
    $date32_min - $datetime64_max,
    $date32_max - $datetime64_min,
    $date32_min - $timestamp64_max,
    $date32_max - $timestamp64_min,
    2,
    $date32_min - $date_max,
    $date32_min - $datetime_max,
    $date32_min - $timestamp_max,
    3,
    $date32_min - $interval64_minus1,
    $date32_max - $interval64_minus1,
    $date32_min - $interval64_plus1,
    $date32_max - $interval64_plus1,
    $date32_min + $interval64_minus1,
    $date32_max + $interval64_minus1,
    $date32_min + $interval64_plus1,
    $date32_max + $interval64_plus1,
    4,
    $date32_min - $interval_minus1,
    $date32_max - $interval_minus1,
    $date32_min - $interval_plus1,
    $date32_max - $interval_plus1,
    $date32_min + $interval_minus1,
    $date32_max + $interval_minus1,
    $date32_min + $interval_plus1,
    $date32_max + $interval_plus1
;

SELECT
    1,
    $datetime64_min - $date32_max,
    $datetime64_max - $date32_min,
    $datetime64_min - $datetime64_max,
    $datetime64_max - $datetime64_min,
    $datetime64_min - $timestamp64_max,
    $datetime64_max - $timestamp64_min,
    2,
    $datetime64_min - $date_max,
    $datetime64_min - $datetime_max,
    $datetime64_min - $timestamp_max,
    3,
    $datetime64_min - $interval64_minus1,
    $datetime64_max - $interval64_minus1,
    $datetime64_min - $interval64_plus1,
    $datetime64_max - $interval64_plus1,
    $datetime64_min + $interval64_minus1,
    $datetime64_max + $interval64_minus1,
    $datetime64_min + $interval64_plus1,
    $datetime64_max + $interval64_plus1,
    4,
    $datetime64_min - $interval_minus1,
    $datetime64_max - $interval_minus1,
    $datetime64_min - $interval_plus1,
    $datetime64_max - $interval_plus1,
    $datetime64_min + $interval_minus1,
    $datetime64_max + $interval_minus1,
    $datetime64_min + $interval_plus1,
    $datetime64_max + $interval_plus1
;

SELECT
    1,
    $timestamp64_min - $date32_max,
    $timestamp64_max - $date32_min,
    $timestamp64_min - $datetime64_max,
    $timestamp64_max - $datetime64_min,
    $timestamp64_min - $timestamp64_max,
    $timestamp64_max - $timestamp64_min,
    2,
    $timestamp64_min - $date_max,
    $timestamp64_min - $datetime_max,
    $timestamp64_min - $timestamp_max,
    3,
    $timestamp64_min - $interval64_minus1,
    $timestamp64_max - $interval64_minus1,
    $timestamp64_min - $interval64_plus1,
    $timestamp64_max - $interval64_plus1,
    $timestamp64_min + $interval64_minus1,
    $timestamp64_max + $interval64_minus1,
    $timestamp64_min + $interval64_plus1,
    $timestamp64_max + $interval64_plus1,
    4,
    $timestamp64_min - $interval_minus1,
    $timestamp64_max - $interval_minus1,
    $timestamp64_min - $interval_plus1,
    $timestamp64_max - $interval_plus1,
    $timestamp64_min + $interval_minus1,
    $timestamp64_max + $interval_minus1,
    $timestamp64_min + $interval_plus1,
    $timestamp64_max + $interval_plus1
;

SELECT
    1,
    $date_max - $date32_min,
    $date_max - $datetime64_min,
    $date_max - $timestamp64_min,
    $date_max - $date32_max,
    $date_max - $datetime64_max,
    $date_max - $timestamp64_max,
    $date_max - $interval64_minus1,
    $date_max + $interval64_minus1,
    $date_max - $interval64_plus1,
    $date_max + $interval64_plus1,
    2,
    $datetime_max - $date32_min,
    $datetime_max - $datetime64_min,
    $datetime_max - $timestamp64_min,
    $datetime_max - $date32_max,
    $datetime_max - $datetime64_max,
    $datetime_max - $timestamp64_max,
    $datetime_max - $interval64_minus1,
    $datetime_max + $interval64_minus1,
    $datetime_max - $interval64_plus1,
    $datetime_max + $interval64_plus1,
    3,
    $timestamp_max - $date32_min,
    $timestamp_max - $datetime64_min,
    $timestamp_max - $timestamp64_min,
    $timestamp_max - $date32_max,
    $timestamp_max - $datetime64_max,
    $timestamp_max - $timestamp64_max,
    $timestamp_max - $interval64_minus1,
    $timestamp_max + $interval64_minus1,
    $timestamp_max - $interval64_plus1,
    $timestamp_max + $interval64_plus1
;

SELECT
    1,
    $interval_min - $interval64_min,
    $interval_min + $interval64_min,
    $interval_min - $interval64_max,
    $interval_min + $interval64_max,
    $interval_max - $interval64_max,
    $interval_max + $interval64_max,
    $interval_max - $interval64_min,
    $interval_max + $interval64_min,
    2,
    $interval64_max - $interval64_min,
    $interval64_min - $interval64_max,
    $interval64_max + $interval64_min,
    $interval64_max + $interval64_max,
    $interval64_min - $interval64_min,
    $interval64_max - $interval64_max
;

SELECT
    0,
    -$interval64_max,
    -$interval64_min,
    -$interval64_zero,
    1,
    $interval64_max * 0,
    0 * $interval64_max,
    2,
    $interval64_max * 1,
    1 * $interval64_max,
    $interval64_max * (-1),
    (-1) * $interval64_max,
    3,
    $interval64_min * 1,
    1 * $interval64_min,
    $interval64_min * (-1),
    (-1) * $interval64_min,
    4,
    $interval64_plus1 * CAST($interval64_max AS int64),
    $interval64_minus1 * CAST($interval64_min AS int64),
    5,
    $interval64_max * $ui64_max,
    $i64_max * $interval64_max,
    $interval64_min * $ui64_max,
    $i64_max * $interval64_min,
    6,
    $interval64_zero * $ui64_max,
    $ui64_max * $interval64_zero,
    $interval64_zero * $i64_max,
    $i64_max * $interval64_zero,
    7,
    $interval64_max / 0,
    $interval64_min / 0,
    $interval64_max / 1,
    $interval64_min / 1,
    $interval64_max / (-1),
    $interval64_min / (-1),
    8,
    $interval64_zero / $ui64_max,
    $interval64_zero / $i64_max,
    $interval64_plus1 / $ui64_max,
    $interval64_plus1 / $i64_max,
    $interval64_minus1 / $ui64_max,
    $interval64_minus1 / $i64_max,
    9,
    $interval64_max / CAST($interval64_max AS int64),
    $interval64_min / CAST($interval64_min AS int64),
    10,
    abs($interval64_max),
    abs($interval64_min),
    abs($interval64_zero),
    11,
    CAST(4294967296l AS interval64) * 4294967296l,
    4294967296ul * CAST(4294967296l AS interval64)
;
