PRAGMA config.flags('OptimizerFlags', 'ExtractCommonPredicatesFromLogicalOps');

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;
$d = 7 > 8;

SELECT
    (($a OR $b) AND $a) == $a
;

SELECT
    (($b OR $a) AND $c AND $b AND ($d OR $c)) == ($c AND $b)
;
