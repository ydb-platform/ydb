pragma config.flags("OptimizerFlags", "ExtractCommonPredicatesFromLogicalOps");

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;

select ($a and $b or $b) == $b;
select ($c and ($b or $a) or $a or $b) == ($a or $b)
