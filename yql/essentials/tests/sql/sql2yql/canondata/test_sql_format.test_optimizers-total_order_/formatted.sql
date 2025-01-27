/* postgres can not */
$x = AsTuple(Double('nan'), 42);

SELECT
    $x == $x
;

SELECT
    $x < $x
;

SELECT
    $x <= $x
;

SELECT
    $x > $x
;

SELECT
    $x >= $x
;

SELECT
    $x != $x
;

$x = AsStruct(Double('nan') AS a, 42 AS b);

SELECT
    $x == $x
;

SELECT
    $x != $x
;

$x = AsTuple(Nothing(ParseType('Int32?')), 1);

SELECT
    $x == $x
;

SELECT
    $x < $x
;

SELECT
    $x <= $x
;

SELECT
    $x > $x
;

SELECT
    $x >= $x
;

SELECT
    $x != $x
;

$x = Nothing(ParseType('Int32?'));

SELECT
    $x == $x
;

SELECT
    $x < $x
;

SELECT
    $x <= $x
;

SELECT
    $x > $x
;

SELECT
    $x >= $x
;

SELECT
    $x != $x
;
