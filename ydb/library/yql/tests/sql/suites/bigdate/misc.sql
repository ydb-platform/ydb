pragma warning("disable","4510");

$date32_min = unwrap(cast(-53375809 as date32));
$date32_max = unwrap(cast(53375807 as date32));
$datetime64_min = unwrap(cast(-4611669897600 as datetime64));
$datetime64_max = unwrap(cast(4611669811199 as datetime64));
$timestamp64_min = unwrap(cast(-4611669897600000000 as timestamp64));
$timestamp64_max = unwrap(cast(4611669811199999999 as timestamp64));
$interval64_min = unwrap(cast(-9223339708799999999 as interval64));
$interval64_max = unwrap(cast(9223339708799999999 as interval64));

$datetime64_zero = unwrap(cast(0 as datetime64));
$timestamp64_zero = unwrap(cast(0 as timestamp64));

$datetime64_day_ml = unwrap(cast(-86401 as datetime64));
$datetime64_day_m  = unwrap(cast(-86400 as datetime64));
$datetime64_day_mr = unwrap(cast(-86399 as datetime64));
$datetime64_day_pl = unwrap(cast(86399 as datetime64));
$datetime64_day_p  = unwrap(cast(86400 as datetime64));
$datetime64_day_pr = unwrap(cast(86401 as datetime64));

$timestamp64_day_ml = unwrap(cast(-86400l*1000000 - 1 as timestamp64));
$timestamp64_day_m  = unwrap(cast(-86400l*1000000 as timestamp64));
$timestamp64_day_mr = unwrap(cast(-86400l*1000000 + 1 as timestamp64));
$timestamp64_day_pl = unwrap(cast(86400l*1000000 - 1 as timestamp64));
$timestamp64_day_p  = unwrap(cast(86400l*1000000 as timestamp64));
$timestamp64_day_pr = unwrap(cast(86400l*1000000 + 1 as timestamp64));

select 1, ListFromRange(date32("1969-12-30"), date32("1970-1-5"))
, 2, ListFromRange(date32("1970-1-3"), date32("1969-12-30"))
, 3, ListFromRange(date32("1969-12-30"), date32("1970-1-5"), interval("P2D"))
, 4, ListFromRange(date32("1969-12-30"), date32("1970-1-5"), interval64("P2D"))
, 5, ListFromRange(date32("1970-1-5"), date32("1969-12-30"))
, 6, ListFromRange(date32("1970-1-5"), date32("1969-12-30"), interval("P2D"))
, 7, ListFromRange(date32("1970-1-5"), date32("1969-12-29"), interval("-P2D"))
, 8, ListFromRange(datetime64("1969-12-31T23:59:57Z"), datetime64("1970-1-1T0:0:3Z"))
, 9, ListFromRange(datetime64("1969-12-31T23:59:57Z"), datetime64("1970-1-1T0:0:3Z"), interval("PT2S"))
, 10, ListFromRange(datetime64("1969-12-31T23:59:57Z"), datetime64("1970-1-1T0:0:3Z"), interval64("PT2S"))
, 11, ListFromRange(timestamp64("1969-12-31T23:59:57Z"), timestamp64("1970-1-1T0:0:3Z"), interval("PT2.5S"))
, 12, ListFromRange($date32_min, $date32_max, interval64("P53375808D"))
, 13, ListFromRange($datetime64_min, $datetime64_max, interval64("P53375808D"))
, 14, ListFromRange($timestamp64_min, $timestamp64_max, interval64("P53375808D"))
, 15, ListFromRange($interval64_min, $interval64_max, interval64("P53375808D"))
;

select -4, Yql::RoundDown($datetime64_min, date32), Yql::RoundUp($datetime64_min, date32)
, Yql::RoundDown($timestamp64_min, date32), Yql::RoundUp($timestamp64_min, date32)
, Yql::RoundDown($timestamp64_min, datetime64), Yql::RoundUp($timestamp64_min, datetime64)

, -3, Yql::RoundDown($datetime64_day_ml, date32), Yql::RoundUp($datetime64_day_ml, date32)
, Yql::RoundDown($timestamp64_day_ml, date32), Yql::RoundUp($timestamp64_day_ml, date32)
, Yql::RoundDown($timestamp64_day_ml, datetime64), Yql::RoundUp($timestamp64_day_ml, datetime64)

, -2, Yql::RoundDown($datetime64_day_m, date32), Yql::RoundUp($datetime64_day_m, date32)
, Yql::RoundDown($timestamp64_day_m, date32), Yql::RoundUp($timestamp64_day_m, date32)
, Yql::RoundDown($timestamp64_day_m, datetime64), Yql::RoundUp($timestamp64_day_m, datetime64)

, -1, Yql::RoundDown($datetime64_day_mr, date32), Yql::RoundUp($datetime64_day_mr, date32)
, Yql::RoundDown($timestamp64_day_mr, date32), Yql::RoundUp($timestamp64_day_mr, date32)
, Yql::RoundDown($timestamp64_day_mr, datetime64), Yql::RoundUp($timestamp64_day_mr, datetime64)

, 0, Yql::RoundDown($datetime64_zero, date32), Yql::RoundUp($datetime64_zero, date32)
, Yql::RoundDown($timestamp64_zero, date32), Yql::RoundUp($timestamp64_zero, date32)
, Yql::RoundDown($timestamp64_zero, datetime64), Yql::RoundUp($timestamp64_zero, datetime64)

, 1, Yql::RoundDown($datetime64_day_pl, date32), Yql::RoundUp($datetime64_day_pl, date32)
, Yql::RoundDown($timestamp64_day_pl, date32), Yql::RoundUp($timestamp64_day_pl, date32)
, Yql::RoundDown($timestamp64_day_pl, datetime64), Yql::RoundUp($timestamp64_day_pl, datetime64)

, 2, Yql::RoundDown($datetime64_day_p, date32), Yql::RoundUp($datetime64_day_p, date32)
, Yql::RoundDown($timestamp64_day_p, date32), Yql::RoundUp($timestamp64_day_p, date32)
, Yql::RoundDown($timestamp64_day_p, datetime64), Yql::RoundUp($timestamp64_day_p, datetime64)

, 3, Yql::RoundDown($datetime64_day_pr, date32), Yql::RoundUp($datetime64_day_pr, date32)
, Yql::RoundDown($timestamp64_day_pr, date32), Yql::RoundUp($timestamp64_day_pr, date32)
, Yql::RoundDown($timestamp64_day_pr, datetime64), Yql::RoundUp($timestamp64_day_pr, datetime64)

, 4, Yql::RoundDown($datetime64_max, date32), Yql::RoundUp($datetime64_max, date32)
, Yql::RoundDown($timestamp64_max, date32), Yql::RoundUp($timestamp64_max, date32)
, Yql::RoundDown($timestamp64_max, datetime64), Yql::RoundUp($timestamp64_max, datetime64)
;
