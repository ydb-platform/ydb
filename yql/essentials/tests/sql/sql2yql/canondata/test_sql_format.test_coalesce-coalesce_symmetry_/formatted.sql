/* postgres can not */
$x = 1 + 0;
$y = 2ul + 0ul;

SELECT
    $x ?? $y
;

SELECT
    $y ?? $x
;

SELECT
    Just($x) ?? $y
;

SELECT
    $y ?? Just($x)
;

SELECT
    $x ?? Just($y)
;

SELECT
    Just($y) ?? $x
;

SELECT
    Just($x) ?? Just($y)
;

SELECT
    Just($y) ?? Just($x)
;
