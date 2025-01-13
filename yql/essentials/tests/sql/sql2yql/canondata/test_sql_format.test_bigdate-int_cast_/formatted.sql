$date32_min = unwrap(CAST(-53375809 AS date32));
$date32_max = unwrap(CAST(53375807 AS date32));
$datetime64_min = unwrap(CAST(-4611669897600 AS datetime64));
$datetime64_max = unwrap(CAST(4611669811199 AS datetime64));
$timestamp64_min = unwrap(CAST(-4611669897600000000 AS timestamp64));
$timestamp64_max = unwrap(CAST(4611669811199999999 AS timestamp64));
$interval64_min = unwrap(CAST(-9223339708799999999 AS interval64));
$interval64_max = unwrap(CAST(9223339708799999999 AS interval64));
$date32_minus1 = unwrap(CAST(-1 AS date32));
$datetime64_minus1 = unwrap(CAST(-1 AS datetime64));
$timestamp64_minus1 = unwrap(CAST(-1 AS timestamp64));
$interval64_minus1 = unwrap(CAST(-1 AS interval64));

-- to signed
SELECT
    1,
    $date32_minus1,
    CAST($date32_minus1 AS int8),
    CAST($date32_minus1 AS int16),
    CAST($date32_minus1 AS int32),
    CAST($date32_minus1 AS int64),
    2,
    $datetime64_minus1,
    CAST($datetime64_minus1 AS int8),
    CAST($datetime64_minus1 AS int16),
    CAST($datetime64_minus1 AS int32),
    CAST($datetime64_minus1 AS int64),
    3,
    $timestamp64_minus1,
    CAST($timestamp64_minus1 AS int8),
    CAST($timestamp64_minus1 AS int16),
    CAST($timestamp64_minus1 AS int32),
    CAST($timestamp64_minus1 AS int64),
    4,
    $interval64_minus1,
    CAST($interval64_minus1 AS int8),
    CAST($interval64_minus1 AS int16),
    CAST($interval64_minus1 AS int32),
    CAST($interval64_minus1 AS int64)
;

-- to unsigned
SELECT
    1,
    CAST($date32_minus1 AS uint32),
    CAST($date32_minus1 AS uint64),
    2,
    CAST($datetime64_minus1 AS uint32),
    CAST($datetime64_minus1 AS uint64),
    3,
    CAST($timestamp64_minus1 AS uint32),
    CAST($timestamp64_minus1 AS uint64),
    4,
    CAST($interval64_minus1 AS uint32),
    CAST($interval64_minus1 AS uint64)
;

-- min/max values
SELECT
    1,
    $date32_min,
    CAST($date32_min AS int32),
    2,
    $datetime64_min,
    CAST($datetime64_min AS int64),
    3,
    $timestamp64_min,
    CAST($timestamp64_min AS int64),
    4,
    $interval64_min,
    CAST($interval64_min AS int64),
    5,
    $date32_max,
    CAST($date32_max AS int32),
    6,
    $datetime64_max,
    CAST($datetime64_max AS int64),
    7,
    $timestamp64_max,
    CAST($timestamp64_max AS int64),
    8,
    $interval64_max,
    CAST($interval64_max AS int64)
;

-- out of range
SELECT
    1,
    CAST(-53375810 AS date32),
    CAST(53375808 AS date32),
    2,
    CAST(-4611669897601 AS datetime64),
    CAST(4611669811200 AS datetime64),
    3,
    CAST(-4611669897600000001 AS timestamp64),
    CAST(4611669811200000000 AS timestamp64),
    4,
    CAST(-9223339708800000000 AS interval64),
    CAST(9223339708800000000 AS interval64)
;

-- insufficient int size
SELECT
    1,
    CAST(unwrap(CAST(32768 AS date32)) AS int16),
    CAST(unwrap(CAST(65536 AS date32)) AS uint16),
    2,
    CAST(unwrap(CAST(32768 AS datetime64)) AS int16),
    CAST(unwrap(CAST(2147483648 AS datetime64)) AS int32),
    3,
    CAST(unwrap(CAST(65536 AS datetime64)) AS uint16),
    CAST(unwrap(CAST(4294967296 AS datetime64)) AS uint32),
    4,
    CAST(unwrap(CAST(32768 AS timestamp64)) AS int16),
    CAST(unwrap(CAST(2147483648 AS timestamp64)) AS int32),
    5,
    CAST(unwrap(CAST(65536 AS timestamp64)) AS uint16),
    CAST(unwrap(CAST(4294967296 AS timestamp64)) AS uint32),
    6,
    CAST(unwrap(CAST(32768 AS interval64)) AS int16),
    CAST(unwrap(CAST(2147483648 AS interval64)) AS int32),
    7,
    CAST(unwrap(CAST(65536 AS interval64)) AS uint16),
    CAST(unwrap(CAST(4294967296 AS interval64)) AS uint32)
;
