/* postgres can not */
$ns = ($set) -> {
    RETURN ListSort(DictKeys($set));
};

$nd = ($dict) -> {
    RETURN ListSort(
        DictItems($dict), ($z) -> {
            RETURN $z.0;
        }
    );
};

SELECT
    $ns(ToSet(AsList(1, 2, 3)))
;

SELECT
    SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4))
;

SELECT
    SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(3, 4))
;

SELECT
    SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(7, 4)))
;

SELECT
    SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4)))
;

SELECT
    $ns(SetIntersection(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))))
;

SELECT
    $nd(
        SetIntersection(
            AsDict(AsTuple(1, 'foo'), AsTuple(3, 'bar')),
            AsDict(AsTuple(1, 'baz'), AsTuple(2, 'qwe')),
            ($_k, $a, $b) -> {
                RETURN AsTuple($a, $b);
            }
        )
    )
;

SELECT
    SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4))
;

SELECT
    SetIncludes(ToSet(AsList(1, 2, 3)), AsList(2, 3))
;

SELECT
    SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4)))
;

SELECT
    SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3)))
;

SELECT
    $ns(SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))))
;

SELECT
    $ns(SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))))
;

SELECT
    $ns(SetUnion(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))))
;

SELECT
    $nd(
        SetUnion(
            AsDict(AsTuple(1, 'foo'), AsTuple(3, 'bar')),
            AsDict(AsTuple(1, 'baz'), AsTuple(2, 'qwe')),
            ($_k, $a, $b) -> {
                RETURN AsTuple($a, $b);
            }
        )
    )
;

SELECT
    $ns(SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))))
;

SELECT
    $nd(
        SetSymmetricDifference(
            AsDict(AsTuple(1, 'foo'), AsTuple(3, 'bar')),
            AsDict(AsTuple(1, 'baz'), AsTuple(2, 'qwe')),
            ($_k, $a, $b) -> {
                RETURN AsTuple($a, $b);
            }
        )
    )
;
