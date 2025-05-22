$date32_min = unwrap(cast(-53375809 as date32));
$date32_max = unwrap(cast(53375807 as date32));

$datetime64_min = unwrap(cast(-4611669897600 as datetime64));
$datetime64_max = unwrap(cast(4611669811199 as datetime64));

$timestamp64_min = unwrap(cast(-4611669897600000000 as timestamp64));
$timestamp64_max = unwrap(cast(4611669811199999999 as timestamp64));

$date32_minus1 = unwrap(cast(-1 as date32));
$datetime64_minus1 = unwrap(cast(-1 as datetime64));
$timestamp64_minus1 = unwrap(cast(-1 as timestamp64));

-- scale up
select 1, cast($date32_minus1 as datetime64), cast($date32_min as datetime64), cast($date32_max as datetime64)
, 2, cast($date32_minus1 as timestamp64), cast($date32_min as timestamp64), cast($date32_max as timestamp64)
, 3, cast($datetime64_minus1 as timestamp64), cast($datetime64_min as timestamp64), cast($datetime64_max as timestamp64);

-- scale down
select 1, cast($timestamp64_minus1 as datetime64), cast($timestamp64_min as datetime64), cast($timestamp64_max as datetime64)
, 2, cast($timestamp64_minus1 as date32), cast($timestamp64_min as date32), cast($timestamp64_max as date32)
, 3, cast($datetime64_minus1 as date32), cast($datetime64_min as date32), cast($datetime64_max as date32);

$date_max_value = 49673l;
$date_max = unwrap(cast($date_max_value - 1 as date));
$datetime_max = unwrap(cast($date_max_value*86400 - 1 as datetime));
$timestamp_max = unwrap(cast($date_max_value*86400*1000000 - 1 as timestamp));
$interval_min = unwrap(cast(-$date_max_value*86400*1000000 + 1 as interval));
$interval_max = unwrap(cast($date_max_value*86400*1000000 - 1 as interval));

-- narrow to bigdate
select 1, cast($date_max as date32), cast($date_max as datetime64), cast($date_max as timestamp64)
, 2 , cast($datetime_max as date32), cast($datetime_max as datetime64), cast($datetime_max as timestamp64)
, 3, cast($timestamp_max as date32), cast($timestamp_max as datetime64), cast($timestamp_max as timestamp64)
, 4, cast($interval_min as interval64), cast($interval_max as interval64);

$date32_val = unwrap(cast($date_max_value - 1 as date32));
$datetime64_val = unwrap(cast($date_max_value*86400 - 1 as datetime64));
$timestamp64_val = unwrap(cast($date_max_value*86400*1000000 - 1 as timestamp64));
$interval64_val_min = unwrap(cast(-$date_max_value*86400*1000000 + 1 as interval64));
$interval64_val_max = unwrap(cast($date_max_value*86400*1000000 - 1 as interval64));

-- bigdate to narrow
select 1, cast($date32_val as date), cast($date32_val as datetime), cast($date32_val as timestamp)
, 2, cast($datetime64_val as date), cast($datetime64_val as datetime), cast($datetime64_val as timestamp)
, 3, cast($timestamp64_val as date), cast($timestamp64_val as datetime), cast($timestamp64_val as timestamp)
, 4, cast($interval64_val_min as interval), cast($interval64_val_max as interval);

select 1, cast($date32_minus1 as date), cast($date32_minus1 as datetime), cast($date32_minus1 as timestamp)
, 2, cast($datetime64_minus1 as date), cast($datetime64_minus1 as datetime), cast($datetime64_minus1 as timestamp)
, 3, cast($timestamp64_minus1 as date), cast($timestamp64_minus1 as datetime), cast($timestamp64_minus1 as timestamp);

-- bigdate to narrow out of range

$date32_big_val = unwrap(cast($date_max_value as date32));
$datetime64_big_val = unwrap(cast($date_max_value*86400 as datetime64));
$timestamp64_big_val = unwrap(cast($date_max_value*86400*1000000 as timestamp64));
$interval64_big_val_min = unwrap(cast(-$date_max_value*86400*1000000 as interval64));
$interval64_big_val_max = unwrap(cast($date_max_value*86400*1000000 as interval64));

select 1, cast($date32_big_val as date), cast($date32_big_val as datetime), cast($date32_big_val as timestamp)
, 2, cast($datetime64_big_val as date), cast($datetime64_big_val as datetime), cast($datetime64_big_val as timestamp)
, 3, cast($timestamp64_big_val as date), cast($timestamp64_big_val as datetime), cast($timestamp64_big_val as timestamp)
, 4, cast($interval64_big_val_min as interval), cast($interval64_big_val_max as interval);
