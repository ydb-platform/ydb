pragma config.flags("OptimizerFlags", "ExtractCommonPredicatesFromLogicalOps");

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;
$d = 7 > 8;

select (($a or $b) and $a) == $a;
select (($b or $a) and $c and $b and ($d or $c)) == ($c and $b);
