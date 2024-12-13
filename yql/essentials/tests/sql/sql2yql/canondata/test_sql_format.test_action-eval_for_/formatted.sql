/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE ACTION $action1($x) AS
    SELECT
        $x
    ;
END DEFINE;

$f = ($i) -> {
    RETURN CAST(Unicode::ToUpper(CAST($i AS Utf8)) AS String);
};

EVALUATE FOR $i IN ListMap(ListFromRange(0, 3), $f) DO
    $action1($i)
;

EVALUATE FOR $i IN ListMap(ListFromRange(0, 0), $f) DO
    $action1($i)
ELSE DO
    $action1(100)
;

EVALUATE FOR $i IN ListMap(ListFromRange(0, 0), $f) DO
    $action1($i)
;

EVALUATE FOR $i IN Yql::Map(
    1 / 1, ($x) -> {
        RETURN AsList($x);
    }
) DO
    $action1($i)
;

EVALUATE FOR $i IN Yql::Map(
    1 / 0, ($x) -> {
        RETURN AsList($x);
    }
) DO
    $action1($i)
;
