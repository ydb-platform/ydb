PRAGMA config.flags('OptimizerFlags', 'ExtractCommonPredicatesFromLogicalOps');

$a = ((2 / 1) > 1) ?? FALSE; -- true
$b = ((3 / 1) > 1) ?? FALSE; -- true
$c = ((4 / 1) < 1) ?? FALSE; -- false
$d = ((5 / 1) > 1); -- true
$e = ((6 / 1) > 1); -- true
$f = ((7 / 1) < 1); -- false

-- abc + ba -> ab(c + true) -> ab
SELECT
    $a AND $b AND $c OR $b AND $a
;

-- ed + abc + def + ba -> ed(true + f) + ab(c + true) -> ed + ab
SELECT
    $e AND $d OR $a AND $b AND $c OR $d AND $e AND $f OR $b AND $a
;
