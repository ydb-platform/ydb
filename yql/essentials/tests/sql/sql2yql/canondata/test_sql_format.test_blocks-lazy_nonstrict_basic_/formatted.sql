USE plato;
PRAGMA yt.DisableOptimizers = "OutHorizontalJoin,HorizontalJoin,MultiHorizontalJoin";
$ns_tolower = ($x) -> (AssumeNonStrict(String::AsciiToLower($x)));
$ns_toupper = ($x) -> (AssumeNonStrict(String::AsciiToUpper($x)));

-- full block
SELECT
    *
FROM
    Input
WHERE
    $ns_tolower(value) > "aaa" AND subkey == "1"
;

-- partial block due to lazy non-strict node
SELECT
    *
FROM
    Input
WHERE
    subkey == "2" AND $ns_toupper(value) <= "ZZZ"
;

-- full block - same non strict is used in first arg of AND
SELECT
    *
FROM
    Input
WHERE
    $ns_toupper(value) >= "AAA" AND $ns_toupper(value) <= "ZZZ" AND subkey == "3"
;
