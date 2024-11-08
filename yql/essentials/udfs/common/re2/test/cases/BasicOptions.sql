/* syntax version 1 */
$options = Re2::Options(true as Utf8);
$match = Re2::Match("[ax]+\d",$options);
$grep = Re2Posix::Grep("a.*",$options);
$capture = Re2::Capture(".*(?P<foo>xa?)(a{2,}).*",$options);
$replace = Re2::Replace("x(a+)x",$options);
$count = Re2::Count("a",$options);
-- regex to find all tokens consisting of letters and digist
-- L stands for "Letters", Nd stands for "Number, decimal digit",
-- see https://en.wikipedia.org/wiki/Unicode_character_property#General_Category
$find_and_consume = Re2::FindAndConsume('([\\pL\\p{Nd}]+)',$options);

SELECT
    value,
    $match(value) AS match,
    $grep(value) AS grep,
    $capture(value) AS capture,
    $capture(value)._1 AS capture_member,
    $replace(value, "b\\1z") AS replace,
    $count(value) AS count,
    $find_and_consume(value) AS tokens
FROM Input;
