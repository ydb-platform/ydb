/* postgres can not */
/* syntax version 1 */
USE plato;

$x = AsStruct(1 AS a);

SELECT
    $x.a
;

$y = AsTuple(2, 3);

SELECT
    $y.1
;

SELECT
    length("foo")
;

SELECT
    Math::Pi()
;

$f = () -> {
    RETURN () -> {
        RETURN AsDict(AsTuple("foo", AsList(AsStruct(AsTuple(1) AS bar))));
    }
};

SELECT
    $f()()["foo"][0].bar.0
;

SELECT
    () -> {
        RETURN 1
    }()
;

$type = Callable<() -> List<Int32>>;

$g = AsStruct(
    Yql::Callable(
        $type, () -> {
            RETURN AsList(1, 2, 3)
        }
    ) AS foo
);

SELECT
    $g.foo()[0]
;
