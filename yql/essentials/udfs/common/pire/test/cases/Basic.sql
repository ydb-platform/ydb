/* syntax version 1 */
$match = Pire::Match("a.*");
$grep = Pire::Grep("axa");
$insensitive_grep = Pire::Grep("(?i)axa");
$multi_match = Pire::MultiMatch(@@a.*
.*a.*
.*a
.*axa.*@@);
$multi_match2 = Pire::MultiMatch(@@YQL.*
QC.*
.*transfer task.*@@);

$capture = Pire::Capture(".*x(a).*");
$capture_many = Pire::Capture(".*x(a+).*");
$replace = Pire::Replace(".*x(a).*");

SELECT
    value,
    $match(value) AS match,
    $grep(value) AS grep,
    $insensitive_grep(value) AS insensitive_grep,
    $multi_match(value) AS multi_match,
    $multi_match(value).0 AS some_multi_match,
    $multi_match2(value) AS multi_match2,
    $multi_match2(value).0 AS some_multi_match2a,
    $multi_match2(value).1 AS some_multi_match2b,
    $multi_match2(value).2 AS some_multi_match2c,
    $capture(value) AS capture,
    $capture_many(value) AS capture_many,
    $replace(value, "b") AS replace,
    $multi_match2(Nothing(String?))
FROM Input;
