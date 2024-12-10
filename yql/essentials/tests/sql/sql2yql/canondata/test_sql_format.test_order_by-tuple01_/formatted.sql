/* postgres can not */
SELECT
    Yql::Sort(
        AsList(3, 2, 1),
        AsTuple(),
        ($_x) -> {
            RETURN AsTuple();
        }
    )
;

SELECT
    Yql::Sort(
        AsList(3, 2, 1),
        AsTuple(TRUE),
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(3, 2, 1),
        TRUE,
        ($x) -> {
            RETURN $x;
        }
    )
;

SELECT
    Yql::Sort(
        AsList(3, 2, 1),
        AsTuple(TRUE),
        ($x) -> {
            RETURN AsTuple($x);
        }
    )
;

SELECT
    Yql::Sort(
        AsList(3, 2, 1),
        AsTuple(TRUE, TRUE),
        ($x) -> {
            RETURN AsTuple($x, $x);
        }
    )
;
