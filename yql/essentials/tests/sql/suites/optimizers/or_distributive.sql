pragma config.flags("OptimizerFlags", "ExtractCommonPredicatesFromLogicalOps");

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;
$d = 7 > 8;
$e = 9 < 10;
$f = 11 > 12;


select (($a and $b) or ($b and $c)) == ($b and ($a or $c));
select (($a and $b) or ($d and $e) or ($b and $c) or ($e and $f)) ==
       ($b and ($a or $c) or $e and ($d or $f));

