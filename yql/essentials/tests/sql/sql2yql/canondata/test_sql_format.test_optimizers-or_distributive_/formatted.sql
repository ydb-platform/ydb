PRAGMA config.flags('OptimizerFlags', 'ExtractCommonPredicatesFromLogicalOps');

$a = 1 > 2;
$b = 3 < 4;
$c = 5 < 6;
$d = 7 > 8;
$e = 9 < 10;
$f = 11 > 12;

SELECT
    (($a AND $b) OR ($b AND $c)) == ($b AND ($a OR $c))
;

SELECT
    (($a AND $b) OR ($d AND $e) OR ($b AND $c) OR ($e AND $f)) == ($b AND ($a OR $c) OR $e AND ($d OR $f))
;
