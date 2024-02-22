$date_min = unwrap(cast(0 as date));
$date_max = unwrap(cast(49672 as date));
$date32_min = unwrap(cast(-53375809 as date32));
$date32_max = unwrap(cast(53375807 as date32));

$datetime64_min = unwrap(cast(-4611669897600 as datetime64));
$datetime64_max = unwrap(cast(4611669811199 as datetime64));

$timestamp64_min = unwrap(cast(-4611669897600000000 as timestamp64));
$timestamp64_max = unwrap(cast(4611669811199999999 as timestamp64));

$interval64_min = unwrap(cast(-9223339708799999999 as interval64));
$interval64_max = unwrap(cast(9223339708799999999 as interval64));


select [$datetime64_min, $date32_min]
-- todo
