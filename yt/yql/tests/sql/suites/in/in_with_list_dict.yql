/* postgres can not */

$l = AsList(1, 2, 3, 800);
$d = AsDict(AsTuple(1, 0), AsTuple(2, 0), AsTuple(3, 0), AsTuple(800, 0));

SELECT key, CAST(key AS int32) IN $l, CAST(key AS int32) IN $d FROM plato.Input;

