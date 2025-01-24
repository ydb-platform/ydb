PRAGMA warning('disable', '4510');

$date32_min = unwrap(CAST(-53375809 AS date32));
$date32_max = unwrap(CAST(53375807 AS date32));
$datetime64_min = unwrap(CAST(-4611669897600 AS datetime64));
$datetime64_max = unwrap(CAST(4611669811199 AS datetime64));
$timestamp64_min = unwrap(CAST(-4611669897600000000 AS timestamp64));
$timestamp64_max = unwrap(CAST(4611669811199999999 AS timestamp64));
$interval64_min = unwrap(CAST(-9223339708799999999 AS interval64));
$interval64_max = unwrap(CAST(9223339708799999999 AS interval64));

SELECT
    1,
    ListFromRange(date32('1969-12-30'), date32('1970-1-5')),
    2,
    ListFromRange(date32('1970-1-3'), date32('1969-12-30')),
    3,
    ListFromRange(date32('1969-12-30'), date32('1970-1-5'), interval('P2D')),
    4,
    ListFromRange(date32('1969-12-30'), date32('1970-1-5'), interval64('P2D')),
    5,
    ListFromRange(date32('1970-1-5'), date32('1969-12-30')),
    6,
    ListFromRange(date32('1970-1-5'), date32('1969-12-30'), interval('P2D')),
    7,
    ListFromRange(date32('1970-1-5'), date32('1969-12-29'), interval('-P2D')),
    8,
    ListFromRange(datetime64('1969-12-31T23:59:57Z'), datetime64('1970-1-1T0:0:3Z')),
    9,
    ListFromRange(datetime64('1969-12-31T23:59:57Z'), datetime64('1970-1-1T0:0:3Z'), interval('PT2S')),
    10,
    ListFromRange(datetime64('1969-12-31T23:59:57Z'), datetime64('1970-1-1T0:0:3Z'), interval64('PT2S')),
    11,
    ListFromRange(timestamp64('1969-12-31T23:59:57Z'), timestamp64('1970-1-1T0:0:3Z'), interval('PT2.5S')),
    12,
    ListFromRange($date32_min, $date32_max, interval64('P53375808D')),
    13,
    ListFromRange($datetime64_min, $datetime64_max, interval64('P53375808D')),
    14,
    ListFromRange($timestamp64_min, $timestamp64_max, interval64('P53375808D')),
    15,
    ListFromRange($interval64_min, $interval64_max, interval64('P53375808D'))
;
