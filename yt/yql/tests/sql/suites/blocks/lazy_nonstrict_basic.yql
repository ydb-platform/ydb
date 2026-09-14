USE plato;

pragma yt.DisableOptimizers="OutHorizontalJoin,HorizontalJoin,MultiHorizontalJoin";


$ns_tolower = ($x) -> (AssumeNonStrict(String::AsciiToLower($x)));
$ns_toupper = ($x) -> (AssumeNonStrict(String::AsciiToUpper($x)));

-- full block
select * from Input where $ns_tolower(value) > "aaa" and subkey == "1";

-- partial block due to lazy non-strict node
select * from Input where subkey == "2" and $ns_toupper(value) <= "ZZZ";

-- full block - same non strict is used in first arg of AND
select * from Input where $ns_toupper(value) >= "AAA" and $ns_toupper(value) <= "ZZZ" and subkey == "3";

