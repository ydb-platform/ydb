/* postgres can not */
/* syntax version 1 */
$l1 = AsList(1, 2, 3, 1, 2, 3);
$l2 = Just($l1);
$l3 = Nothing(TypeOf($l2));

SELECT
    $l1,
    $l2,
    $l3
;

$p = ($x) -> {
    RETURN $x < 3;
};

SELECT
    ListFilter($l1, $p),
    ListFilter($l2, $p),
    ListFilter($l3, $p)
;

$m = ($x) -> {
    RETURN $x * 2;
};

SELECT
    ListMap($l1, $m),
    ListMap($l2, $m),
    ListMap($l3, $m)
;

$f = ($x) -> {
    RETURN AsList($x, $x * 2);
};

SELECT
    ListFlatMap($l1, $f),
    ListFlatMap($l2, $f),
    ListFlatMap($l3, $f)
;

SELECT
    ListSkipWhile($l1, $p),
    ListSkipWhile($l2, $p),
    ListSkipWhile($l3, $p)
;

SELECT
    ListTakeWhile($l1, $p),
    ListTakeWhile($l2, $p),
    ListTakeWhile($l3, $p)
;

SELECT
    ListExtend($l1, $l1),
    ListExtend($l2, $l2),
    ListExtend($l2, $l3),
    ListExtend($l3, $l3)
;

$ls1 = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$ls2 = Just($ls1);
$ls3 = Nothing(TypeOf($ls2));

SELECT
    ListUnionAll($ls1, $ls1),
    ListUnionAll($ls2, $ls2),
    ListUnionAll($ls2, $ls3),
    ListUnionAll($ls3, $ls3)
;
