/* postgres can not */
$id = ($x) -> {
    $f = Yql::Callable(
        CallableType(0, TypeOf($x)), () -> {
            RETURN $x;
        }
    );
    RETURN $f();
};

$vt1 = ParseType('Variant<Int32,Uint32?>');
$vt2 = ParseType('Variant<Int64,Uint32>');

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) < VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) < VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) < VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) < VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) < VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) < VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) < VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) < VARIANT (1u, '1', $vt2)
    )
;

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) <= VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) <= VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) <= VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) <= VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) <= VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) <= VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) <= VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) <= VARIANT (1u, '1', $vt2)
    )
;

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) == VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) == VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) == VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) == VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) == VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) == VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) == VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) == VARIANT (1u, '1', $vt2)
    )
;

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) != VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) != VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) != VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) != VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) != VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) != VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) != VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) != VARIANT (1u, '1', $vt2)
    )
;

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) > VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) > VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) > VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) > VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) > VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) > VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) > VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) > VARIANT (1u, '1', $vt2)
    )
;

SELECT
    AsTuple(
        VARIANT (1, '0', $vt1) >= VARIANT ($id(1u), '0', $vt2),
        VARIANT (1, '0', $vt1) >= VARIANT (2u, '0', $vt2),
        VARIANT (1, '0', $vt1) >= VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) >= VARIANT ($id(1u), '1', $vt2),
        VARIANT (1, '1', $vt1) >= VARIANT (2u, '1', $vt2),
        VARIANT (2, '1', $vt1) >= VARIANT (1u, '1', $vt2),
        VARIANT (1, '1', $vt1) >= VARIANT ($id(1u), '0', $vt2),
        VARIANT (2u / 0u, '1', $vt1) >= VARIANT (1u, '1', $vt2)
    )
;
