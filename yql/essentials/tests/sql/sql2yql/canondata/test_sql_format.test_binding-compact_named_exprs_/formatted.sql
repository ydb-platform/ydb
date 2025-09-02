/* yt can not */
PRAGMA CompactNamedExprs;

$foo = 1 + 2;
$a, $b = AsTuple(1 + 3, 2 + 5);
$l = ($x) -> ($x + $foo);

SELECT
    $foo,
    $a,
    $b,
    $l(123)
;
