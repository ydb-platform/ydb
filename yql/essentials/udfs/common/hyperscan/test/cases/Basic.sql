/* syntax version 1 */
$match = Hyperscan::Match("a.*");
$grep = Hyperscan::Grep("axa");
$insensitive_grep = Hyperscan::Grep("(?i)axa");
$multi_match = Hyperscan::MultiMatch(@@a.*
.*a.*
.*a
.*axa.*@@);
$multi_match2 = Hyperscan::MultiMatch(@@YQL.*
QC.*
.*transfer task.*@@);

$capture = Hyperscan::Capture(".*a{2}.*");
$capture_many = Hyperscan::Capture(".*x(a+).*");
$replace = Hyperscan::Replace("xa");
$backtracking_grep = Hyperscan::BacktrackingGrep("(?<!xa)ax");

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
    $backtracking_grep(value) as backtracking
FROM Input;
