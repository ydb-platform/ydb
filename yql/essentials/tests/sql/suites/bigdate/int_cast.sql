$date32_min = unwrap(cast(-53375809 as date32));
$date32_max = unwrap(cast(53375807 as date32));

$datetime64_min = unwrap(cast(-4611669897600 as datetime64));
$datetime64_max = unwrap(cast(4611669811199 as datetime64));

$timestamp64_min = unwrap(cast(-4611669897600000000 as timestamp64));
$timestamp64_max = unwrap(cast(4611669811199999999 as timestamp64));

$interval64_min = unwrap(cast(-9223339708799999999 as interval64));
$interval64_max = unwrap(cast(9223339708799999999 as interval64));

$date32_minus1 = unwrap(cast(-1 as date32));
$datetime64_minus1 = unwrap(cast(-1 as datetime64));
$timestamp64_minus1 = unwrap(cast(-1 as timestamp64));
$interval64_minus1 = unwrap(cast(-1 as interval64));

-- to signed
select 1, $date32_minus1, cast($date32_minus1 as int8), cast($date32_minus1 as int16), cast($date32_minus1 as int32), cast($date32_minus1 as int64)
, 2, $datetime64_minus1, cast($datetime64_minus1 as int8), cast($datetime64_minus1 as int16), cast($datetime64_minus1 as int32), cast($datetime64_minus1 as int64)
, 3, $timestamp64_minus1, cast($timestamp64_minus1 as int8), cast($timestamp64_minus1 as int16), cast($timestamp64_minus1 as int32), cast($timestamp64_minus1 as int64)
, 4, $interval64_minus1, cast($interval64_minus1 as int8), cast($interval64_minus1 as int16), cast($interval64_minus1 as int32), cast($interval64_minus1 as int64);

-- to unsigned
select 1, cast($date32_minus1 as uint32), cast($date32_minus1 as uint64)
, 2, cast($datetime64_minus1 as uint32), cast($datetime64_minus1 as uint64)
, 3, cast($timestamp64_minus1 as uint32), cast($timestamp64_minus1 as uint64)
, 4, cast($interval64_minus1 as uint32), cast($interval64_minus1 as uint64);

-- min/max values
select 1, $date32_min, cast($date32_min as int32)
, 2, $datetime64_min, cast($datetime64_min as int64)
, 3, $timestamp64_min, cast($timestamp64_min as int64)
, 4, $interval64_min, cast($interval64_min as int64)
, 5, $date32_max, cast($date32_max as int32)
, 6, $datetime64_max, cast($datetime64_max as int64)
, 7, $timestamp64_max, cast($timestamp64_max as int64)
, 8, $interval64_max, cast($interval64_max as int64);

-- out of range
select 1, cast(-53375810 as date32), cast(53375808 as date32)
, 2, cast(-4611669897601 as datetime64), cast(4611669811200 as datetime64)
, 3, cast(-4611669897600000001 as timestamp64), cast(4611669811200000000 as timestamp64)
, 4, cast(-9223339708800000000 as interval64), cast(9223339708800000000 as interval64);

-- insufficient int size
select 1, cast(unwrap(cast(32768 as date32)) as int16), cast(unwrap(cast(65536 as date32)) as uint16)
, 2, cast(unwrap(cast(32768 as datetime64)) as int16), cast(unwrap(cast(2147483648 as datetime64)) as int32)
, 3, cast(unwrap(cast(65536 as datetime64)) as uint16), cast(unwrap(cast(4294967296 as datetime64)) as uint32)
, 4, cast(unwrap(cast(32768 as timestamp64)) as int16), cast(unwrap(cast(2147483648 as timestamp64)) as int32)
, 5, cast(unwrap(cast(65536 as timestamp64)) as uint16), cast(unwrap(cast(4294967296 as timestamp64)) as uint32)
, 6, cast(unwrap(cast(32768 as interval64)) as int16), cast(unwrap(cast(2147483648 as interval64)) as int32)
, 7, cast(unwrap(cast(65536 as interval64)) as uint16), cast(unwrap(cast(4294967296 as interval64)) as uint32);
