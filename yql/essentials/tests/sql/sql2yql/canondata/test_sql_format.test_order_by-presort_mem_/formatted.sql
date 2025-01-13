/* postgres can not */
SELECT
    Yql::Sort(
        AsList(
            AsTuple(3, 1),
            AsTuple(1, 2),
            AsTuple(1, 3),
        ),
        TRUE,
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(
            AsTuple(3, 1),
            AsTuple(1, 1),
            AsTuple(1, 3),
        ),
        AsTuple(TRUE, FALSE),
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(
            AsList(3, 1),
            AsList(1, 2),
            AsList(2, 3),
            AsList(1, 2, 3)
        ),
        TRUE,
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(
            AsTuple(1, AsList(3, 1)),
            AsTuple(1, AsList(1, 2)),
            AsTuple(1, AsList(2, 3)),
            AsTuple(1, AsList(1, 2, 3)),
            AsTuple(2, AsList(3, 1)),
            AsTuple(2, AsList(1, 2)),
            AsTuple(2, AsList(2, 3)),
            AsTuple(2, AsList(1, 2, 3))
        ),
        AsTuple(TRUE, FALSE),
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(
            AsTuple(1, AsList(3, 1)),
            AsTuple(1, AsList(1, 2)),
            AsTuple(1, AsList(2, 3)),
            AsTuple(1, AsList(1, 2, 3)),
            AsTuple(2, AsList(3, 1)),
            AsTuple(2, AsList(1, 2)),
            AsTuple(2, AsList(2, 3)),
            AsTuple(2, AsList(1, 2, 3))
        ),
        AsTuple(TRUE, TRUE),
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(
            AsTuple(1, AsList(3, 1)),
            AsTuple(1, AsList(1, 2)),
            AsTuple(1, AsList(2, 3)),
            AsTuple(1, AsList(1, 2, 3)),
            AsTuple(2, AsList(3, 1)),
            AsTuple(2, AsList(1, 2)),
            AsTuple(2, AsList(2, 3)),
            AsTuple(2, AsList(1, 2, 3))
        ),
        FALSE,
        ($x) -> {
            RETURN $x;
        }
    )
;
