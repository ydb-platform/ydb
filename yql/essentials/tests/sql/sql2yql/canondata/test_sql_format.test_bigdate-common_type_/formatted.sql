$date_val = unwrap(CAST(1 AS date));
$date32_val = unwrap(CAST(-1 AS date32));
$datetime_val = unwrap(CAST(86400 AS datetime));
$datetime64_val = unwrap(CAST(-86400 AS datetime64));
$timestamp_val = unwrap(CAST(86400l * 1000000 AS timestamp));
$timestamp64_val = unwrap(CAST(-86400l * 1000000 AS timestamp64));
$interval_val = unwrap(CAST(1 AS interval));
$interval64_val = unwrap(CAST(-1 AS interval64));

SELECT
    1,
    [$date_val, $datetime_val],
    [$date_val, $timestamp_val],
    [$date_val, $date32_val],
    [$date_val, $datetime64_val],
    [$date_val, $timestamp64_val],
    2,
    [$datetime_val, $date_val],
    [$datetime_val, $timestamp_val],
    [$datetime_val, $date32_val],
    [$datetime_val, $datetime64_val],
    [$datetime_val, $timestamp64_val],
    3,
    [$timestamp_val, $date_val],
    [$timestamp_val, $datetime_val],
    [$timestamp_val, $date32_val],
    [$timestamp_val, $datetime64_val],
    [$timestamp_val, $timestamp64_val],
    4,
    [$date32_val, $date_val],
    [$date32_val, $datetime_val],
    [$date32_val, $timestamp_val],
    [$date32_val, $datetime64_val],
    [$date32_val, $timestamp64_val],
    5,
    [$datetime64_val, $date_val],
    [$datetime64_val, $datetime_val],
    [$datetime64_val, $timestamp_val],
    [$datetime64_val, $date32_val],
    [$datetime64_val, $timestamp64_val],
    6,
    [$timestamp64_val, $date_val],
    [$timestamp64_val, $datetime_val],
    [$timestamp64_val, $timestamp_val],
    [$timestamp64_val, $date32_val],
    [$timestamp64_val, $datetime64_val],
    7,
    [$date_val, $datetime_val, $timestamp_val, $date32_val, $datetime64_val, $timestamp64_val]
;

SELECT
    [unwrap(CAST(1 AS interval)), unwrap(CAST(-1 AS interval64))]
;

$datetime_values = [$date_val, $date32_val, $datetime_val, $datetime64_val, $timestamp_val, $timestamp64_val];
$interval_values = [$interval_val, $interval64_val];

SELECT
    ListSort(DictKeys(ToSet($datetime_values))),
    ListSort(DictKeys(ToSet($interval_values)))
;
