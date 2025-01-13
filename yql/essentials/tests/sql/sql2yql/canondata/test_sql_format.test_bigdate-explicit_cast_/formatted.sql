$date32_min = unwrap(CAST(-53375809 AS date32));
$date32_max = unwrap(CAST(53375807 AS date32));
$datetime64_min = unwrap(CAST(-4611669897600 AS datetime64));
$datetime64_max = unwrap(CAST(4611669811199 AS datetime64));
$timestamp64_min = unwrap(CAST(-4611669897600000000 AS timestamp64));
$timestamp64_max = unwrap(CAST(4611669811199999999 AS timestamp64));
$date32_minus1 = unwrap(CAST(-1 AS date32));
$datetime64_minus1 = unwrap(CAST(-1 AS datetime64));
$timestamp64_minus1 = unwrap(CAST(-1 AS timestamp64));

-- scale up
SELECT
    1,
    CAST($date32_minus1 AS datetime64),
    CAST($date32_min AS datetime64),
    CAST($date32_max AS datetime64),
    2,
    CAST($date32_minus1 AS timestamp64),
    CAST($date32_min AS timestamp64),
    CAST($date32_max AS timestamp64),
    3,
    CAST($datetime64_minus1 AS timestamp64),
    CAST($datetime64_min AS timestamp64),
    CAST($datetime64_max AS timestamp64)
;

-- scale down
SELECT
    1,
    CAST($timestamp64_minus1 AS datetime64),
    CAST($timestamp64_min AS datetime64),
    CAST($timestamp64_max AS datetime64),
    2,
    CAST($timestamp64_minus1 AS date32),
    CAST($timestamp64_min AS date32),
    CAST($timestamp64_max AS date32),
    3,
    CAST($datetime64_minus1 AS date32),
    CAST($datetime64_min AS date32),
    CAST($datetime64_max AS date32)
;

$date_max_value = 49673l;
$date_max = unwrap(CAST($date_max_value - 1 AS date));
$datetime_max = unwrap(CAST($date_max_value * 86400 - 1 AS datetime));
$timestamp_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS timestamp));
$interval_min = unwrap(CAST(-$date_max_value * 86400 * 1000000 + 1 AS interval));
$interval_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS interval));

-- narrow to bigdate
SELECT
    1,
    CAST($date_max AS date32),
    CAST($date_max AS datetime64),
    CAST($date_max AS timestamp64),
    2,
    CAST($datetime_max AS date32),
    CAST($datetime_max AS datetime64),
    CAST($datetime_max AS timestamp64),
    3,
    CAST($timestamp_max AS date32),
    CAST($timestamp_max AS datetime64),
    CAST($timestamp_max AS timestamp64),
    4,
    CAST($interval_min AS interval64),
    CAST($interval_max AS interval64)
;

$date32_val = unwrap(CAST($date_max_value - 1 AS date32));
$datetime64_val = unwrap(CAST($date_max_value * 86400 - 1 AS datetime64));
$timestamp64_val = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS timestamp64));
$interval64_val_min = unwrap(CAST(-$date_max_value * 86400 * 1000000 + 1 AS interval64));
$interval64_val_max = unwrap(CAST($date_max_value * 86400 * 1000000 - 1 AS interval64));

-- bigdate to narrow
SELECT
    1,
    CAST($date32_val AS date),
    CAST($date32_val AS datetime),
    CAST($date32_val AS timestamp),
    2,
    CAST($datetime64_val AS date),
    CAST($datetime64_val AS datetime),
    CAST($datetime64_val AS timestamp),
    3,
    CAST($timestamp64_val AS date),
    CAST($timestamp64_val AS datetime),
    CAST($timestamp64_val AS timestamp),
    4,
    CAST($interval64_val_min AS interval),
    CAST($interval64_val_max AS interval)
;

SELECT
    1,
    CAST($date32_minus1 AS date),
    CAST($date32_minus1 AS datetime),
    CAST($date32_minus1 AS timestamp),
    2,
    CAST($datetime64_minus1 AS date),
    CAST($datetime64_minus1 AS datetime),
    CAST($datetime64_minus1 AS timestamp),
    3,
    CAST($timestamp64_minus1 AS date),
    CAST($timestamp64_minus1 AS datetime),
    CAST($timestamp64_minus1 AS timestamp)
;

-- bigdate to narrow out of range
$date32_big_val = unwrap(CAST($date_max_value AS date32));
$datetime64_big_val = unwrap(CAST($date_max_value * 86400 AS datetime64));
$timestamp64_big_val = unwrap(CAST($date_max_value * 86400 * 1000000 AS timestamp64));
$interval64_big_val_min = unwrap(CAST(-$date_max_value * 86400 * 1000000 AS interval64));
$interval64_big_val_max = unwrap(CAST($date_max_value * 86400 * 1000000 AS interval64));

SELECT
    1,
    CAST($date32_big_val AS date),
    CAST($date32_big_val AS datetime),
    CAST($date32_big_val AS timestamp),
    2,
    CAST($datetime64_big_val AS date),
    CAST($datetime64_big_val AS datetime),
    CAST($datetime64_big_val AS timestamp),
    3,
    CAST($timestamp64_big_val AS date),
    CAST($timestamp64_big_val AS datetime),
    CAST($timestamp64_big_val AS timestamp),
    4,
    CAST($interval64_big_val_min AS interval),
    CAST($interval64_big_val_max AS interval)
;
