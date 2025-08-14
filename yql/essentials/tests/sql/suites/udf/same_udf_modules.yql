/* postgres can not */
$value = "xaaxaaxaa";
$match = Re2::Match("[ax]+\\d");
$grep = Re2Posix::Grep("a.*");
$capture = Re2::Capture(".*(?P<foo>xa?)(a{2,}).*");
$replace = Re2::Replace("x(a+)x");
$count = Re2::Count("a");
 
SELECT
    $match($value) AS match,
    $grep($value) AS grep,
    $capture($value) AS capture,
    $capture($value)._1 AS capture_member,
    $replace($value, "b\\1z") AS replace,
    $count($value) AS count;
