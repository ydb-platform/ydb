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

-- scale up
select 1, cast($date32_minus1 as datetime64), cast($date32_min as datetime64), cast($date32_max as datetime64)
, 2, cast($date32_minus1 as timestamp64), cast($date32_min as timestamp64), cast($date32_max as timestamp64)
, 3, cast($datetime64_minus1 as timestamp64), cast($datetime64_min as timestamp64), cast($datetime64_max as timestamp64);

-- scale down
select 1, cast($timestamp64_minus1 as datetime64), cast($timestamp64_min as datetime64), cast($timestamp64_max as datetime64)
, 2, cast($timestamp64_minus1 as date32), cast($timestamp64_min as date32), cast($timestamp64_max as date32)
, 3, cast($datetime64_minus1 as date32), cast($datetime64_min as date32), cast($datetime64_max as date32);


-- bigdate to narrow

-- narrow to bigdate
-- todo
