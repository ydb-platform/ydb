/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a));
$f = AGGREGATION_FACTORY("median");

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a
                    }
                )
            )
        )
    )
;

$f = AGGREGATION_FACTORY("percentile", 0.9);

SELECT
    Yql::Aggregate(
        $t, AsTuple(), AsTuple(
            AsTuple(
                AsAtom("res"), $f(
                    ListItemType(TypeOf($t)), ($z) -> {
                        RETURN $z.a
                    }
                )
            )
        )
    )
;

USE plato;

INSERT INTO @a
SELECT
    *
FROM
    as_table($t)
;

COMMIT;

SELECT
    AGGREGATE_BY(a, $f)
FROM
    @a
;

SELECT
    AGGREGATE_BY(DISTINCT a, $f)
FROM
    @a
;
