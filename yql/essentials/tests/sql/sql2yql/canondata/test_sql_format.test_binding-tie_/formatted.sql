/* syntax version 1 */
/* postgres can not */
$a, $b, $c = AsTuple(1, 5u, 'test');

SELECT
    $a,
    $b,
    $c
;
