$date32_min = cast(-53375809 as date32);
$date32_max = cast(53375807 as date32);

$datetime64_min = cast(-4611669897600 as datetime64);
$datetime64_max = cast(4611669811199 as datetime64);

$timestamp64_min = cast(-4611669897600000000 as timestamp64);
$timestamp64_max = cast(4611669811199999999 as timestamp64);

$interval64_min = cast(-9223339708799999999 as interval64);
$interval64_max = cast(9223339708799999999 as interval64);

-- scale down timestamp64
select cast($timestamp64_min as datetime64);
-- todo


-- scale up date32
select cast($date32_min as datetime64);
-- todo



