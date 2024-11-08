/* syntax version 1 */
$match = Re2::Match("[ax]+\d");
$grep = Re2Posix::Grep("a.*");
$capture = Re2::Capture(".*(?P<foo>xa?)(a{2,}).*");
$replace = Re2::Replace("x(a+)x");
$count = Re2::Count("a");
-- regex to find all tokens consisting of letters and digist
-- L stands for "Letters", Nd stands for "Number, decimal digit",
-- see https://en.wikipedia.org/wiki/Unicode_character_property#General_Category
$find_and_consume = Re2::FindAndConsume('([\\pL\\p{Nd}]+)');

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
