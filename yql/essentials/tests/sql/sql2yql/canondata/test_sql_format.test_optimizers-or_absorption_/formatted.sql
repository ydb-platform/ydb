PRAGMA config.flags('OptimizerFlags', 'ExtractCommonPredicatesFromLogicalOps');

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;

SELECT
    ($a AND $b OR $b) == $b
;

SELECT
    ($c AND ($b OR $a) OR $a OR $b) == ($a OR $b)
;
