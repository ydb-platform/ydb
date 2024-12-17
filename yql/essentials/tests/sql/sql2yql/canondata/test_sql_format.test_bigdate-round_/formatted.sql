PRAGMA warning('disable', '4510');

$date_max_value = 49673l;
$date_max = unwrap(CAST($date_max_value - 1 AS date));
$datetime_max = unwrap(CAST($date_max_value * 86400 - 1 AS datetime));
$timestamp_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS timestamp));
$date32_min = unwrap(CAST(-53375809 AS date32));
$date32_max = unwrap(CAST(53375807 AS date32));
$date32_plus1 = unwrap(CAST(1 AS date32));
$datetime64_min = unwrap(CAST(-4611669897600 AS datetime64));
$datetime64_max = unwrap(CAST(4611669811199 AS datetime64));
$timestamp64_min = unwrap(CAST(-4611669897600000000 AS timestamp64));
$timestamp64_max = unwrap(CAST(4611669811199999999 AS timestamp64));
$timestamp64_max_narrow = unwrap(CAST($timestamp_max AS timestamp64));
$datetime64_max_narrow = unwrap(CAST($datetime_max AS datetime64));
$datetime64_zero = unwrap(CAST(0 AS datetime64));
$timestamp64_zero = unwrap(CAST(0 AS timestamp64));
$datetime64_minus1 = unwrap(CAST(-1 AS datetime64));
$timestamp64_minus1 = unwrap(CAST(-1 AS timestamp64));
$timestamp64_2xx31 = unwrap(CAST(2147483648 AS timestamp64));
$datetime64_day_ml = unwrap(CAST(-86401 AS datetime64));
$datetime64_day_m = unwrap(CAST(-86400 AS datetime64));
$datetime64_day_mr = unwrap(CAST(-86399 AS datetime64));
$datetime64_day_pl = unwrap(CAST(86399 AS datetime64));
$datetime64_day_p = unwrap(CAST(86400 AS datetime64));
$datetime64_day_pr = unwrap(CAST(86401 AS datetime64));
$timestamp64_day_ml = unwrap(CAST(-86400l * 1000000 - 1 AS timestamp64));
$timestamp64_day_m = unwrap(CAST(-86400l * 1000000 AS timestamp64));
$timestamp64_day_mr = unwrap(CAST(-86400l * 1000000 + 1 AS timestamp64));
$timestamp64_day_pl = unwrap(CAST(86400l * 1000000 - 1 AS timestamp64));
$timestamp64_day_p = unwrap(CAST(86400l * 1000000 AS timestamp64));
$timestamp64_day_pr = unwrap(CAST(86400l * 1000000 + 1 AS timestamp64));

-- bigdate to bigdate
SELECT
    -4,
    Yql::RoundDown($datetime64_min, date32),
    Yql::RoundUp($datetime64_min, date32),
    Yql::RoundDown($timestamp64_min, date32),
    Yql::RoundUp($timestamp64_min, date32),
    Yql::RoundDown($timestamp64_min, datetime64),
    Yql::RoundUp($timestamp64_min, datetime64),
    -3,
    Yql::RoundDown($datetime64_day_ml, date32),
    Yql::RoundUp($datetime64_day_ml, date32),
    Yql::RoundDown($timestamp64_day_ml, date32),
    Yql::RoundUp($timestamp64_day_ml, date32),
    Yql::RoundDown($timestamp64_day_ml, datetime64),
    Yql::RoundUp($timestamp64_day_ml, datetime64),
    -2,
    Yql::RoundDown($datetime64_day_m, date32),
    Yql::RoundUp($datetime64_day_m, date32),
    Yql::RoundDown($timestamp64_day_m, date32),
    Yql::RoundUp($timestamp64_day_m, date32),
    Yql::RoundDown($timestamp64_day_m, datetime64),
    Yql::RoundUp($timestamp64_day_m, datetime64),
    -1,
    Yql::RoundDown($datetime64_day_mr, date32),
    Yql::RoundUp($datetime64_day_mr, date32),
    Yql::RoundDown($timestamp64_day_mr, date32),
    Yql::RoundUp($timestamp64_day_mr, date32),
    Yql::RoundDown($timestamp64_day_mr, datetime64),
    Yql::RoundUp($timestamp64_day_mr, datetime64),
    0,
    Yql::RoundDown($datetime64_zero, date32),
    Yql::RoundUp($datetime64_zero, date32),
    Yql::RoundDown($timestamp64_zero, date32),
    Yql::RoundUp($timestamp64_zero, date32),
    Yql::RoundDown($timestamp64_zero, datetime64),
    Yql::RoundUp($timestamp64_zero, datetime64),
    1,
    Yql::RoundDown($datetime64_day_pl, date32),
    Yql::RoundUp($datetime64_day_pl, date32),
    Yql::RoundDown($timestamp64_day_pl, date32),
    Yql::RoundUp($timestamp64_day_pl, date32),
    Yql::RoundDown($timestamp64_day_pl, datetime64),
    Yql::RoundUp($timestamp64_day_pl, datetime64),
    2,
    Yql::RoundDown($datetime64_day_p, date32),
    Yql::RoundUp($datetime64_day_p, date32),
    Yql::RoundDown($timestamp64_day_p, date32),
    Yql::RoundUp($timestamp64_day_p, date32),
    Yql::RoundDown($timestamp64_day_p, datetime64),
    Yql::RoundUp($timestamp64_day_p, datetime64),
    3,
    Yql::RoundDown($datetime64_day_pr, date32),
    Yql::RoundUp($datetime64_day_pr, date32),
    Yql::RoundDown($timestamp64_day_pr, date32),
    Yql::RoundUp($timestamp64_day_pr, date32),
    Yql::RoundDown($timestamp64_day_pr, datetime64),
    Yql::RoundUp($timestamp64_day_pr, datetime64),
    4,
    Yql::RoundDown($datetime64_max, date32),
    Yql::RoundUp($datetime64_max, date32),
    Yql::RoundDown($timestamp64_max, date32),
    Yql::RoundUp($timestamp64_max, date32),
    Yql::RoundDown($timestamp64_max, datetime64),
    Yql::RoundUp($timestamp64_max, datetime64)
;

-- bigdate to narrowdate
SELECT
    -1,
    Yql::RoundDown($datetime64_minus1, date),
    Yql::RoundUp($datetime64_minus1, date),
    Yql::RoundDown($timestamp64_minus1, date),
    Yql::RoundUp($timestamp64_minus1, date),
    Yql::RoundDown($timestamp64_minus1, datetime),
    Yql::RoundUp($timestamp64_minus1, datetime),
    0,
    Yql::RoundDown($datetime64_zero, date),
    Yql::RoundUp($datetime64_zero, date),
    Yql::RoundDown($timestamp64_zero, date),
    Yql::RoundUp($timestamp64_zero, date),
    Yql::RoundDown($timestamp64_zero, datetime),
    Yql::RoundUp($timestamp64_zero, datetime),
    1,
    Yql::RoundDown($datetime64_day_pl, date),
    Yql::RoundUp($datetime64_day_pl, date),
    Yql::RoundDown($timestamp64_day_pl, date),
    Yql::RoundUp($timestamp64_day_pl, date),
    Yql::RoundDown($timestamp64_day_pl, datetime),
    Yql::RoundUp($timestamp64_day_pl, datetime),
    2,
    Yql::RoundDown($datetime64_day_p, date),
    Yql::RoundUp($datetime64_day_p, date),
    Yql::RoundDown($timestamp64_day_p, date),
    Yql::RoundUp($timestamp64_day_p, date),
    Yql::RoundDown($timestamp64_day_p, datetime),
    Yql::RoundUp($timestamp64_day_p, datetime),
    3,
    Yql::RoundDown($datetime64_day_pr, date),
    Yql::RoundUp($datetime64_day_pr, date),
    Yql::RoundDown($timestamp64_day_pr, date),
    Yql::RoundUp($timestamp64_day_pr, date),
    Yql::RoundDown($timestamp64_day_pr, datetime),
    Yql::RoundUp($timestamp64_day_pr, datetime),
    4,
    Yql::RoundDown($datetime64_max_narrow, date),
    Yql::RoundUp($datetime64_max_narrow, date),
    Yql::RoundDown($timestamp64_max_narrow, date),
    Yql::RoundUp($timestamp64_max_narrow, date),
    Yql::RoundDown($timestamp64_max_narrow, datetime),
    Yql::RoundUp($timestamp64_max_narrow, datetime),
    5,
    Yql::RoundDown($datetime64_max, date),
    Yql::RoundUp($datetime64_max, date),
    Yql::RoundDown($timestamp64_max, date),
    Yql::RoundUp($timestamp64_max, date),
    Yql::RoundDown($timestamp64_max, datetime),
    Yql::RoundUp($timestamp64_max, datetime),
    6,
    Yql::RoundDown($date32_plus1, date),
    Yql::RoundUp($date32_plus1, date),
    Yql::RoundDown($date32_plus1, datetime),
    Yql::RoundUp($date32_plus1, datetime),
    Yql::RoundDown($date32_plus1, timestamp),
    Yql::RoundUp($date32_plus1, timestamp)
;

-- from narrowdate
SELECT
    0,
    Yql::RoundDown($datetime_max, date),
    Yql::RoundUp($datetime_max, date),
    Yql::RoundDown($datetime_max, date32),
    Yql::RoundUp($datetime_max, date32),
    1,
    Yql::RoundDown($timestamp_max, date),
    Yql::RoundUp($timestamp_max, date),
    Yql::RoundDown($timestamp_max, date32),
    Yql::RoundUp($timestamp_max, date32),
    2,
    Yql::RoundDown($timestamp_max, datetime),
    Yql::RoundUp($timestamp_max, datetime),
    Yql::RoundDown($timestamp_max, datetime64),
    Yql::RoundUp($timestamp_max, datetime64)
;

SELECT
    0,
    Yql::RoundDown($timestamp64_2xx31, date32),
    Yql::RoundUp($timestamp64_2xx31, date32),
    Yql::RoundDown($timestamp64_2xx31, datetime64),
    Yql::RoundUp($timestamp64_2xx31, datetime64),
    1,
    Yql::RoundDown($date_max, date),
    Yql::RoundUp($date_max, date32),
    2,
    Yql::RoundDown($date_max, datetime),
    Yql::RoundUp($date_max, datetime64),
    3,
    Yql::RoundDown($date_max, timestamp),
    Yql::RoundUp($date_max, timestamp64),
    4,
    Yql::RoundDown($datetime_max, datetime),
    Yql::RoundUp($datetime_max, datetime64),
    5,
    Yql::RoundDown($datetime_max, timestamp),
    Yql::RoundUp($datetime_max, timestamp64),
    6,
    Yql::RoundDown($timestamp_max, timestamp),
    Yql::RoundUp($timestamp_max, timestamp64),
    10,
    Yql::RoundDown($date32_min, date32),
    Yql::RoundUp($date32_max, date32),
    11,
    Yql::RoundDown($date32_min, datetime64),
    Yql::RoundUp($date32_max, datetime64),
    12,
    Yql::RoundDown($date32_min, timestamp64),
    Yql::RoundUp($date32_max, timestamp64),
    13,
    Yql::RoundDown($datetime64_min, datetime64),
    Yql::RoundUp($datetime64_max, datetime64),
    14,
    Yql::RoundDown($datetime64_min, timestamp64),
    Yql::RoundUp($datetime64_max, timestamp64),
    15,
    Yql::RoundDown($timestamp64_min, timestamp64),
    Yql::RoundUp($timestamp64_max, timestamp64)
;
