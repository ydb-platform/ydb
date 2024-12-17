/* postgres can not */
/* syntax version 1 */
$f = ($x, $y?) -> ($x + ($y ?? 0));

SELECT
    $f(1),
    $f(2, 3)
;
